#define MLOG_FACSARRAY   /* need to define before include mlog .h files */

#include "parse_conf.h"
#include "plfs_internal.h"
#include "mlog.h"
#include "FlatFileFS.h"
#include "ContainerFS.h"
#include "SmallFileFS.h"


void
set_default_mount(PlfsMount *pmnt)
{
    pmnt->statfs = pmnt->syncer_ip = NULL;
    pmnt->statfs_io.prefix = NULL;
    pmnt->statfs_io.store = NULL;
    pmnt->file_type = CONTAINER;
    pmnt->fs_ptr = &containerfs;
    pmnt->max_writers = 4;
    pmnt->glib_buffer_mbs = 16;
    pmnt->max_smallfile_containers = 32;
    pmnt->checksum = (unsigned)-1;
    pmnt->backspec = pmnt->canspec = pmnt->shadowspec = NULL;
    pmnt->attached = pmnt->nback = pmnt->ncanback = pmnt->nshadowback = 0;
    pmnt->backstore = NULL;
    pmnt->backends = pmnt->canonical_backends = pmnt->shadow_backends = NULL;
    pmnt->err_msg = NULL;
}

void
set_default_confs(PlfsConf *pconf)
{
    pconf->num_hostdirs = 32;
    pconf->threadpool_size = 8;
    pconf->direct_io = 0;
    pconf->lazy_stat = 1;
    pconf->lazy_droppings = 1;
    pconf->compress_contiguous = 1;    
    pconf->err_msg = NULL;
    pconf->buffer_mbs = 64;
    pconf->read_buffer_mbs = 64;
    pconf->global_summary_dir = NULL;
    pconf->global_sum_io.prefix = NULL;
    pconf->global_sum_io.store = NULL;
    pconf->test_metalink = 0;
    /* default mlog settings */
    pconf->mlog_flags = MLOG_LOGPID;
    pconf->mlog_defmask = MLOG_WARN;
    pconf->mlog_stderrmask = MLOG_CRIT;
    pconf->mlog_file_base = NULL;
    pconf->mlog_file = NULL;
    pconf->mlog_msgbuf_size = 4096;
    pconf->mlog_syslogfac = LOG_USER;
    pconf->mlog_setmasks = NULL;
    pconf->tmp_mnt = NULL;
    pconf->fuse_crash_log = NULL;
}

/**
 * find_replace: finds and replaces all substrings within a string, 
 * in place for speed...used for parsing the config file
 *
 * @param subject the string to search through
 * @param search the string to find in subject
 * @param replace the string to insert into subject
 */
void 
find_replace(string& subject, const string& search, const string& replace) {
    size_t pos = 0;
    while((pos = subject.find(search, pos)) != string::npos) {
         subject.replace(pos, search.length(), replace);
         pos += replace.length();
    }
}

string *
insert_mount_point(PlfsConf *pconf, PlfsMount *pmnt);

void
setup_mlog_facnamemask(char *masks);

static void
setup_mlog(PlfsConf *pconf);

// a helper function that expands %t, %p, %h in mlog file name
string
expand_macros(const char *target) {
    ostringstream oss;
    for(size_t i = 0; i < strlen(target); i++) {
        if (target[i] != '%') {
            oss << target[i];
        } else {
            switch(target[++i]) {
                case 'h':
                    oss << Util::hostname();
                    break;
                case 'p':
                    oss << getpid();
                    break;
                case 't':
                    oss << time(NULL);
                    break;
                default:
                    oss << "%";
                    oss << target[i];
                    break;
            }
        }
    }
    return oss.str();
}

// a list of all valid keys in plfsrc, this must be updated whenever
// a new key is added to plfsrc
string Valid_Keys[] = {
    "global_params", "mount_type", "mount_point", "backends", "location",
    "workload", "num_hostdirs", "threadpool_size", "index_buffer_mbs",
    "glib_buffer_mbs", "read_buffer_mbs", "lazy_stat", "lazy_droppings", 
    "syncer_ip", "global_summary_dir", "statfs", "test_metalink", 
    "mlog_defmask", "mlog_setmasks", "mlog_stderrmask", "mlog_stderr", 
    "mlog_file", "mlog_msgbuf_size", "mlog_syslog", "mlog_syslogfac", 
    "mlog_ucon", "include", "type", "compress_contiguous"
};

/*
 * This function checks to see if the current YAML::Node contains only keys
 * that we care about. Unknown keys should cause PLFS to spit out an error
 * rather than being silently ignored.
 * This is a bit nasty as it drills through the entire tree recursively
 * but it will catch any unknowns in one pass
 *
 * Returns true if all keys are valid
 *
 * Returns false if unknown keys are found and sets bad_key to an error
 * message that points out what key is invalid
 */
bool
is_valid_node(const YAML::Node node, string** bad_key) {
    set<string> key_list(Valid_Keys, 
                         Valid_Keys + 
                         (sizeof(Valid_Keys) / sizeof(Valid_Keys[0]))
                        );
    string key;
    string err = "\nUnknown key in plfsrc: ";
    if(node.IsMap()) {
        for(YAML::const_iterator it=node.begin();it!=node.end();it++) {
            if(!it->first.IsNull()) {
                key = it->first.as<string>();
                if(!is_valid_node(node[key],bad_key)) // recurse
                    return false;
                if(key_list.find(key) == key_list.end()) {
                    err.append(key);
                    *bad_key = new string (err);
                    return false; // this is an unknown key
                }
            }
        }
    }
    else if (node.IsSequence()) {
        for(int i = 0; i < node.size(); i++)
            if(!is_valid_node(node[i],bad_key)) // recurse
                return false;
    }
    return true; // all keys are valid
}


/**
 * 
 * These templates define the mapping between a YAML::Node populated by
 * keys/values from the config file to the PlfsConf and PlfsMount structures
 * 
 * NOTE: The return of this is a new PlfsConf structure from scratch -
 * if you set the return of this to an existing PlfsConf it will overwrite it
 * 
 */
namespace YAML {
   template<typename T>
   bool conv(const Node& node, T& rhs) {
       if(node.IsNull()) return false;
       istringstream temp(node.as<string>());
       temp >> rhs;
       temp >> std::ws;
       if (temp.fail() || !temp.eof()) {
           cerr << "Plfsrc invalid option value: " << node << endl;
           return false;
       }
       else
           return true;
   }
   template<>
   struct convert<PlfsConf> {
       static bool decode(const Node& node, PlfsConf& pconf) {
           if(node["global_params"]) {
               set_default_confs(&pconf);
               if(node["num_hostdirs"]) {
                   if(!conv(node["num_hostdirs"],pconf.num_hostdirs) || 
                      pconf.num_hostdirs > MAX_HOSTDIRS ||
                      pconf.num_hostdirs <= 0)
                       pconf.err_msg = new string ("Illegal num_hostdirs");
               }
               if(node["threadpool_size"]) {
                   if(!conv(node["threadpool_size"], pconf.threadpool_size) ||
                      pconf.threadpool_size < 1)
                       pconf.err_msg = new string ("Illegal threadpool_size");
                   pconf.threadpool_size = max(pconf.threadpool_size, 1);
               }
               if(node["lazy_stat"]) {
                   if(!conv(node["lazy_stat"], pconf.lazy_stat)) 
                       pconf.err_msg = new string ("Illegal lazy_stat");
               }
               if(node["lazy_droppings"]) {
                   if(!conv(node["lazy_droppings"], pconf.lazy_droppings))
                       pconf.err_msg = new string ("Illegal lazy_droppings");
               }
               if(node["compress_contiguous"]) {
                   if(!conv(node["compress_contiguous"],pconf.compress_contiguous))
                       pconf.err_msg = new string ("Illegal compress_contiguous");
               }
               if(node["index_buffer_mbs"]) {
                   if(!conv(node["index_buffer_mbs"], pconf.buffer_mbs) ||
                      pconf.buffer_mbs < 0)
                       pconf.err_msg = new string ("Illegal index_buffer_mbs");
               }
               if(node["read_buffer_mbs"]) {
                   if(!conv(node["read_buffer_mbs"],pconf.read_buffer_mbs) ||
                      pconf.read_buffer_mbs < 0)
                       pconf.err_msg = new string ("Illegal read_buffer_mbs");
               }
               if(node["global_summary_dir"]) {
                   string temp;
                   if(!conv(node["global_summary_dir"],temp) || temp.c_str()[0] != '/') 
                       pconf.err_msg = new string ("Illegal global_summary_dir");
                   else {
                       pconf.global_summary_dir = strdup(temp.c_str());
                       pconf.global_sum_io.prefix = strdup(temp.c_str());
                   }
               }
               if(node["test_metalink"]) { 
                   if(!conv(node["test_metalink"],pconf.test_metalink))
                       pconf.err_msg = new string ("Illegal test_metalink");
               }
               if(node["mlog_stderr"]) {
                   bool temp;
                   if(!conv(node["mlog_stderr"],temp))
                       pconf.err_msg = new string ("Illegal mlog_stderr");
                   else {
                       if (temp)
                           pconf.mlog_flags |= MLOG_STDERR;
                       else
                           pconf.mlog_flags &= ~MLOG_STDERR;
                   }
               }
               if(node["mlog_ucon"]) {
                   bool temp;
                   if(!conv(node["mlog_ucon"],temp))
                       pconf.err_msg = new string ("Illegal mlog_ucon");
                   else {
                       if (temp)
                           pconf.mlog_flags |= (MLOG_UCON_ON|MLOG_UCON_ENV);
                       else
                           pconf.mlog_flags &= ~(MLOG_UCON_ON|MLOG_UCON_ENV);
                   }
               }
               if(node["mlog_syslog"]) {
                   bool temp;
                   if(!conv(node["mlog_syslog"],temp))
                       pconf.err_msg = new string ("Illegal mlog_syslog");
                   else {
                       if (temp)
                           pconf.mlog_flags |= MLOG_SYSLOG;
                       else
                           pconf.mlog_flags &= ~MLOG_SYSLOG;
                   }
               }
               if(node["mlog_defmask"]) {
                   string temp;
                   if(!conv(node["mlog_defmask"],temp))
                       pconf.err_msg = new string ("Illegal mlog_defmask");
                   else {
                       pconf.mlog_defmask = mlog_str2pri(temp.c_str());
                       if(pconf.mlog_defmask < 0)
                           pconf.err_msg = new string ("Bad mlog_defmask value");
                   }
               }
               if(node["mlog_stderrmask"]) {
                   string temp;
                   if(!conv(node["mlog_stderrmask"],temp))
                       pconf.err_msg = new string ("Illegal mlog_stderrmask");
                   else {
                       pconf.mlog_stderrmask = mlog_str2pri(temp.c_str());
                       if(pconf.mlog_stderrmask < 0)
                           pconf.err_msg = new string ("Bad mlog_stderrmask value");
                   }
               }
               if(node["mlog_file"]) {
                   string temp;
                   if(!conv(node["mlog_file"],temp))
                       pconf.err_msg = new string ("Illegal mlog_file");
                   else {
                       if(!(strchr(temp.c_str(),'%') != NULL))
                           pconf.mlog_file = strdup(temp.c_str());
                       else {
                           pconf.mlog_file_base = strdup(temp.c_str());
                           pconf.mlog_file = 
                               strdup(expand_macros(temp.c_str()).c_str());
                       }
                   }
               }
               if(node["mlog_msgbuf_size"]) {
                   if(!conv(node["mlog_msgbuf_size"],pconf.mlog_msgbuf_size) ||
                      pconf.mlog_msgbuf_size < 256)
                       pconf.err_msg = new string ("Illegal mlog_msgbuf_size");
               }
               if(node["mlog_syslogfac"]) {
                   int temp = 
                       atoi(&node["mlog_syslogfac"].as<string>().c_str()[5]);
                   switch (temp) {
                   case 0:
                       pconf.mlog_syslogfac = LOG_LOCAL0;
                       break;
                   case 1:
                       pconf.mlog_syslogfac = LOG_LOCAL1;
                       break;
                   case 2:
                       pconf.mlog_syslogfac = LOG_LOCAL2;
                       break;
                   case 3:
                       pconf.mlog_syslogfac = LOG_LOCAL3;
                       break;
                   case 4:
                       pconf.mlog_syslogfac = LOG_LOCAL4;
                       break;
                   case 5:
                       pconf.mlog_syslogfac = LOG_LOCAL5;
                       break;
                   case 6:
                       pconf.mlog_syslogfac = LOG_LOCAL6;
                       break;
                   case 7:
                       pconf.mlog_syslogfac = LOG_LOCAL7;
                       break;
                   default:
                       pconf.err_msg = 
                           new string("bad mlog_syslogfac value");
                   }
               }
               if(node["mlog_setmasks"]) {
                   string temp;
                   if(!conv(node["mlog_setmasks"],temp))
                       pconf.err_msg = new string ("Illegal mlog_setmasks");
                   else {
                       find_replace(temp, " ", ",");
                       pconf.mlog_setmasks = strdup(temp.c_str());
                   }
               }
               if(node["fuse_crash_log"]) {
                   if(!conv(node["fuse_crash_log"],pconf.fuse_crash_log))
                       pconf.err_msg = new string ("Illegal fuse_crash_log");
               } 
               return true;
           }
           pconf.err_msg = 
               new string("Decode global_params called on unknown node");
           return false;
       }
   };
   
   template<>
   struct convert<PlfsMount> {
       static bool decode(const Node& node, PlfsMount& pmntp) {
           if (node["mount_point"]) {
               set_default_mount(&pmntp);
               pmntp.mnt_pt = node["mount_point"].as<string>();
               Util::fast_tokenize(pmntp.mnt_pt.c_str(),pmntp.mnt_tokens);
               if(node["max_smallfile_containers"]) {
                   if(!conv(node["max_smallfile_containers"],
                            pmntp.max_smallfile_containers) ||
                      pmntp.max_smallfile_containers < 1) {
                       pmntp.err_msg = 
                           new string("Illegal max_smallfile_containers");
                   }
               }
               if(node["workload"]) {
                   string temp;
                   if(!conv(node["workload"],temp)) {
                       pmntp.err_msg = new string("Illegal workload");
                   }
                   else if(temp == "file_per_proc" || temp == "n-n") {
                       pmntp.file_type = FLAT_FILE;
                       pmntp.fs_ptr = &flatfs;
                   }
                   else if(temp == "shared_file" || temp == "n-1") {
                       pmntp.file_type = CONTAINER;
                       pmntp.fs_ptr = &containerfs;
                   }
                   else if(temp == "small_file" || temp == "1-n") {
                       pmntp.file_type = SMALL_FILE;
                       pmntp.fs_ptr = new SmallFileFS(pmntp.max_smallfile_containers);
                   }
                   else {
                       pmntp.err_msg = new string("Unknown workload type");
                   }
               }
               if(node["max_writers"]) {
                   if(!conv(node["max_writers"],pmntp.max_writers) ||
                      pmntp.max_writers < 1) {
                       pmntp.err_msg = new string("Illegal max_writers");
                   }
               }
               if(node["glib_buffer_mbs"]) {
                   if(!conv(node["glib_buffer_mbs"],pmntp.glib_buffer_mbs) ||
                      pmntp.glib_buffer_mbs < 0) {
                       pmntp.err_msg = new string("Illegal glib_buffer_mbs");
                   }
               }
               if(node["statfs"]) {
                   if(!conv(node["statfs"],*pmntp.statfs)) {
                       pmntp.err_msg = new string("Illegal statfs");
                   }
                   pmntp.statfs_io.prefix = strdup(pmntp.statfs->c_str());
               }
               if(node["backends"]) {
                   string backspec;
                   string canspec;
                   string shadowspec;
                   string temp;
                   string temp_loc;
                   for(unsigned i = 0; i < node["backends"].size(); i++) {
                       if(!conv(node["backends"][i]["location"],temp_loc)) {
                           pmntp.err_msg = new string("Illegal backend location");
                           break;
                       }
                       if(node["backends"][i]["type"]) {
                           if(!conv(node["backends"][i]["type"],temp)) {
                               pmntp.err_msg = new string("Illegal backend type");
                               break;
                           }
                           if(temp == "canonical") {
                               if (!canspec.empty())
                                   canspec.append(",");
                               canspec.append(temp_loc);
                           }
                           else if(temp == "shadow") {
                               if (!shadowspec.empty())
                                   shadowspec.append(",");
                               shadowspec.append(temp_loc);
                           }
                           else {
                               pmntp.err_msg = new string("Unknown backend type");
                           }
                       }
                       else {
                           if (!backspec.empty())
                               backspec.append(",");
                           backspec.append(temp_loc);
                       }
                   }
                   if (!backspec.empty()) {
                       pmntp.backspec = strdup(backspec.c_str());
                       pmntp.checksum = 
                           (unsigned)Container::hashValue(backspec.c_str());
                   }
                   if (!canspec.empty())
                       pmntp.canspec = strdup(canspec.c_str());
                   if (!shadowspec.empty())
                       pmntp.shadowspec = strdup(shadowspec.c_str());
               }
               if(node["syncer_ip"]) {
                   if(!conv(node["syncer_ip"],*pmntp.syncer_ip)) {
                       pmntp.err_msg = new string("Illegal syncer_ip");
                   }
                   mlog(MLOG_DBG, "Discovered syncer_ip %s\n",
                       pmntp.syncer_ip->c_str());
               }
               return true;
           }
           pmntp.err_msg = new string("Decode mount called on non-mount node\n");
           return false;
       }
   };
}

PlfsConf *
parse_conf(YAML::Node cnode, string file, PlfsConf *pconf)
{
    bool top_of_stack = (pconf==NULL); // this recurses.  Remember who is top.
    pair<set<string>::iterator, bool> insert_ret;
    if (!pconf) {
        pconf = new PlfsConf; /* XXX: and if new/malloc fails? */
        set_default_confs(pconf);
    }
    insert_ret = pconf->files.insert(file);
    mlog(MLOG_DBG, "Parsing %s", file.c_str());
    if (insert_ret.second == false) {
        pconf->err_msg = new string("include file included more than once");
        return pconf;
    }
    // only special case here is include
    // if any includes, recurse
    // probably get rid of parse_conf_keyval entirely
    for (unsigned i = 0; i < cnode.size(); i++) {
        // make sure there aren't any unknown entries
        if (!is_valid_node(cnode[i],&pconf->err_msg))
            break;
        if (cnode[i]["include"]) {
            if(cnode[i]["include"].IsNull()) {
                pconf->err_msg = new string("Include file invalid");
                break;
            }
            string inc_file = cnode[i]["include"].as<string>();
            YAML::Node inc_node;
            try { inc_node = YAML::LoadFile(inc_file); }
            catch (exception& e) {
                pconf->err_msg = new string("Include file not found.");
                break;
            }
            if (!pconf->err_msg)
                pconf = parse_conf(inc_node, inc_file, pconf);
        }
        else if (cnode[i]["global_params"]) {
            PlfsConf temp_conf = *pconf; // save current mount params
            try { *pconf = cnode[i].as<PlfsConf>(); }
            catch (exception &e) {
                pconf->err_msg = 
                    new string(e.what());
                break;
            }
            if (pconf->err_msg)
                break;
            // now restore saved parameters to pconf
            pconf->files = temp_conf.files;
            pconf->backends = temp_conf.backends;
            pconf->mnt_pts = temp_conf.mnt_pts;
        }
        else if (cnode[i]["mount_point"]) {
            pconf->tmp_mnt = new PlfsMount;
            set_default_mount(pconf->tmp_mnt);
            try { *pconf->tmp_mnt = cnode[i].as<PlfsMount>(); }
            catch (exception &e) {
                pconf->err_msg = 
                    new string(e.what());
                break;
            }
            if (pconf->tmp_mnt->err_msg) {
                pconf->err_msg = pconf->tmp_mnt->err_msg;
                break;
            }
            pconf->err_msg = insert_mount_point(pconf, pconf->tmp_mnt);
        }
        else {
            ostringstream error;
            error << "Unknown config parameter: " << cnode[i] << endl;
            pconf->err_msg = 
                    new string(error.str());
            break;
        }
    }
    mlog(MLOG_DBG, "Got EOF from parsing conf %s",file.c_str());
    if (top_of_stack) {
        if (!pconf->err_msg && pconf->mnt_pts.size()<=0 && top_of_stack) {
            pconf->err_msg = new string("No mount points defined.");
        }
        pconf->file = file; // restore top-level plfsrc
    }
    if(pconf->err_msg) {
        mlog(MLOG_DBG, "Error in the conf file: %s", pconf->err_msg->c_str());
        ostringstream error_msg;
        error_msg << "Parse error in " << file << ": "
                  << pconf->err_msg->c_str() << endl;
        delete pconf->err_msg;
        pconf->err_msg = new string(error_msg.str());
    }
    assert(pconf);
    mlog(MLOG_DBG, "Successfully parsed conf file");
    return pconf;
}

// get a pointer to a struct holding plfs configuration values
// this is called multiple times but should be set up initially just once
// it reads the map and creates tokens for the expression that
// matches logical and the expression used to resolve into physical
// boy, I wonder if we have to protect this.  In fuse, it *should* get
// done at mount time so that will init it before threads show up
// in adio, there are no threads.  should be OK.
PlfsConf *
get_plfs_conf()
{
    static pthread_mutex_t confmutex = PTHREAD_MUTEX_INITIALIZER;
    static PlfsConf *pconf = NULL;   /* note static */

    pthread_mutex_lock(&confmutex);
    if (pconf ) {
        pthread_mutex_unlock(&confmutex);
        return pconf;
    }
    /*
     * bring up a simple mlog here so we can collect early error messages
     * before we've got access to all the mlog config info from file.
     * we'll replace with the proper settings once we've got the conf
     * file loaded and the command line args parsed...
     * XXXCDC: add code to check environment vars for non-default levels
     */
    if (mlog_open((char *)"plfsinit",
                  /* don't count the null at end of mlog_facsarray */
                  sizeof(mlog_facsarray)/sizeof(mlog_facsarray[0]) - 1,
                  MLOG_WARN, MLOG_WARN, NULL, 0, MLOG_LOGPID, 0) == 0) {
        setup_mlog_facnamemask(NULL);
    }
    map<string,string> confs;
    vector<string> possible_files;
    // three possible plfsrc locations:
    // first, env PLFSRC, 2nd $HOME/.plfsrc, 3rd /etc/plfsrc
    if ( getenv("PLFSRC") ) {
        string env_file = getenv("PLFSRC");
        possible_files.push_back(env_file);
    }
    if ( getenv("HOME") ) {
        string home_file = getenv("HOME");
        home_file.append("/.plfsrc");
        possible_files.push_back(home_file);
    }
    possible_files.push_back("/etc/plfsrc");
    // try to parse each file until one works
    // the C++ way to parse like this is istringstream (bleh)
    for( size_t i = 0; i < possible_files.size(); i++ ) {
        string file = possible_files[i];
        YAML::Node cnode;
        try { 
            cnode = YAML::LoadFile(file.c_str());
        }
        catch (exception& e) {
            mlog(MLOG_ERR, "%s", e.what());
            mlog(MLOG_ERR, ".plfsrc file that caused exception: %s", file.c_str());
            exit(1);
        }
        PlfsConf *tmppconf = parse_conf(cnode,file,NULL);
        if(tmppconf) {
            if(tmppconf->err_msg) {
                pthread_mutex_unlock(&confmutex);
                return tmppconf;
            } else {
                pconf = tmppconf;
            }
        }
        break;
    }
    if (pconf) {
        setup_mlog(pconf);
    }
    pthread_mutex_unlock(&confmutex);
    return pconf;
}

/**
 * plfs_mlogargs: manage mlog command line args (override plfsrc).
 *
 * @param mlargc argc (in if mlargv, out if !mlargv)
 * @param mlargv NULL if reading back old value, otherwise value to save
 * @return the mlog argv[]
 */
char **
plfs_mlogargs(int *mlargc, char **mlargv)
{
    static int mlac = 0;
    static char **mlav = NULL;
    if (mlargv) {
        mlac = *mlargc;    /* read back */
        mlav = mlargv;
    } else {
        *mlargc = mlac;    /* set */
    }
    return(mlav);
}

/**
 * plfs_mlogtag: allow override of default mlog tag for apps that
 * can support it.
 *
 * @param newtag the new tag to use, or NULL just to read the tag
 * @return the current tag
 */
char *
plfs_mlogtag(char *newtag)
{
    static char *tag = NULL;
    if (newtag) {
        tag = newtag;
    }
    return((tag) ? tag : (char *)"plfs");
}

/**
 * setup_mlog_facnamemask: setup the mlog facility names and inital
 * mask.    helper function for setup_mlog() and get_plfs_conf(), the
 * latter for the early mlog init before the plfsrc is read.
 *
 * @param masks masks in mlog_setmasks() format, or NULL
 */
void
setup_mlog_facnamemask(char *masks)
{
    int lcv;
    /* name facilities */
    for (lcv = 0; mlog_facsarray[lcv] != NULL ; lcv++) {
        /* can't fail, as we preallocated in mlog_open() */
        if (lcv == 0) {
            continue;    /* don't mess with the default facility */
        }
        (void) mlog_namefacility(lcv, (char *)mlog_facsarray[lcv],
                                 (char *)mlog_lfacsarray[lcv]);
    }
    /* finally handle any mlog_setmasks() calls */
    if (masks != NULL) {
        mlog_setmasks(masks, -1);
    }
}

/**
 * setup_mlog: setup and open the mlog, as per default config, augmented
 * by plfsrc, and then by command line args
 *
 * XXX: we call parse_conf_keyval with a NULL pmntp... shouldn't be
 * a problem because we restrict the parser to "mlog_" style key values
 * (so it will never touch that).
 *
 * @param pconf the config we are going to use
 */
static void
setup_mlog(PlfsConf *pconf)
{
    static const char *menvs[] = { "PLFS_MLOG_STDERR", "PLFS_MLOG_UCON",
                                   "PLFS_MLOG_SYSLOG", "PLFS_MLOG_DEFMASK",
                                   "PLFS_MLOG_STDERRMASK", "PLFS_MLOG_FILE",
                                   "PLFS_MLOG_MSGBUF_SIZE",
                                   "PLFS_MLOG_SYSLOGFAC",
                                   "PLFS_MLOG_SETMASKS", 0
                                 };
    int lcv, mac;
    char *ev, *p, **mav, *start;
    char tmpbuf[64];   /* must be larger than any envs in menvs[] */
    const char *level;
    YAML::Node mlog_args;
    PlfsConf temp_conf, default_conf; // to only override new mlog params
    mlog_args["global_params"] = ""; // set this as a global_params node
    set_default_confs(&default_conf);
    
    /* read in any config from the environ */
    for (lcv = 0 ; menvs[lcv] != NULL ; lcv++) {
        ev = getenv(menvs[lcv]);
        if (ev == NULL) {
            continue;
        }
        strcpy(tmpbuf, menvs[lcv] + sizeof("PLFS_")-1);
        for (p = tmpbuf ; *p ; p++) {
            if (isupper(*p)) {
                *p = tolower(*p);
            }
        }
        mlog_args[tmpbuf] = ev;
    }
    /* recover command line arg key/value pairs, if any */
    mav = plfs_mlogargs(&mac, NULL);
    if (mac) {
        for (lcv = 0 ; lcv < mac ; lcv += 2) {
            start = mav[lcv];
            if (start[0] == '-' && start[1] == '-') {
                start += 2;    /* skip "--" */
            }
            mlog_args[start] = mav[lcv+1];
        }
    }
    /* simplified high-level env var config, part 1 (WHERE) */
    ev = getenv("PLFS_DEBUG_WHERE");
    if (ev) {
        mlog_args["mlog_file"] = ev;
    }
    // now we have a YAML::Node with the parameters we want to override
    try { temp_conf = mlog_args.as<PlfsConf>(); }
    catch(exception &e) {
        mlog(MLOG_WARN, "mlog config parsing error: %s", e.what());
    }
    // now compare to the default config and update any that differ
    // this is a bit messy/verbose...but is simple to change if need be
    if (temp_conf.mlog_flags != default_conf.mlog_flags)
        pconf->mlog_flags &= temp_conf.mlog_flags;
    if (temp_conf.mlog_defmask != default_conf.mlog_defmask)
        pconf->mlog_defmask = temp_conf.mlog_defmask;
    if (temp_conf.mlog_file != NULL) {
        pconf->mlog_file = strdup(temp_conf.mlog_file);
        if (temp_conf.mlog_file_base)
            pconf->mlog_file_base = strdup(temp_conf.mlog_file_base);
    }
    if (temp_conf.mlog_msgbuf_size != default_conf.mlog_msgbuf_size)
        pconf->mlog_msgbuf_size = temp_conf.mlog_msgbuf_size;
    if (temp_conf.mlog_syslogfac != default_conf.mlog_syslogfac)
        pconf->mlog_syslogfac = temp_conf.mlog_syslogfac;
    /* end of part 1 of simplified high-level env var config */
    /* shutdown early mlog config so we can replace with the real one ... */
    mlog_close();
    /* now we are ready to mlog_open ... */
    if (mlog_open(plfs_mlogtag(NULL),
                  /* don't count the null at end of mlog_facsarray */
                  sizeof(mlog_facsarray)/sizeof(mlog_facsarray[0]) - 1,
                  pconf->mlog_defmask, pconf->mlog_stderrmask,
                  pconf->mlog_file, pconf->mlog_msgbuf_size,
                  pconf->mlog_flags, pconf->mlog_syslogfac) < 0) {
        fprintf(stderr, "mlog_open: failed.  Check mlog params.\n");
        /* XXX: keep going without log?   or abort/exit? */
        exit(1);
    }
    setup_mlog_facnamemask(pconf->mlog_setmasks);
    /* simplified high-level env var config, part 2 (LEVEL,WHICH) */
    level = getenv("PLFS_DEBUG_LEVEL");
    if (level && mlog_str2pri((char *)level) == -1) {
        mlog(MLOG_WARN, "PLFS_DEBUG_LEVEL error: bad level: %s", level);
        level = NULL;   /* reset to default */
    }
    ev = getenv("PLFS_DEBUG_WHICH");
    if (ev == NULL) {
        if (level != NULL) {
            mlog_setmasks((char *)level, -1);  /* apply to all facs */
        }
    } else {
        while (*ev) {
            start = ev;
            while (*ev != 0 && *ev != ',') {
                ev++;
            }
            snprintf(tmpbuf, sizeof(tmpbuf), "%.*s=%s", (int)(ev - start),
                     start, (level) ? level : "DBUG");
            mlog_setmasks(tmpbuf, -1);
            if (*ev == ',') {
                ev++;
            }
        }
    }
    /* end of part 2 of simplified high-level env var config */
    mlog(PLFS_INFO, "mlog init complete");
#if 0
    /* XXXCDC: FOR LEVEL DEBUG */
    mlog(PLFS_EMERG, "test emergy log");
    mlog(PLFS_ALERT, "test alert log");
    mlog(PLFS_CRIT, "test crit log");
    mlog(PLFS_ERR, "test err log");
    mlog(PLFS_WARN, "test warn log");
    mlog(PLFS_NOTE, "test note log");
    mlog(PLFS_INFO, "test info log");
    /* XXXCDC: END LEVEL DEBUG */
#endif
    return;
}
