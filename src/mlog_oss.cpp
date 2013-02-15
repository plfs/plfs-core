#include "mlog_oss.h"

mss::mlog_oss::mlog_oss() {
    noLog = true;
}

mss::mlog_oss::mlog_oss(int lvl) {
    if (MLOG_NEVERLOG == 0 &&
            mlog_filter(lvl)) {
        noLog = false;
    } else {
        noLog = true;
    }
}

std::string mss::mlog_oss::str() {
    return oss.str();
}

bool mss::mlog_oss::getNoLog() {
    return noLog;
}

void mss::mlog_oss::insert_os(const std::ostream& xin) {
    oss << xin;
}

/*
std::ostream& mss::mlog_oss::operator<< (bool val) {
    check_lvl(val);
    return oss;
}

std::ostream& mss::mlog_oss::operator<< (short val) {
    check_lvl(val);
    return oss;
}

std::ostream& mss::mlog_oss::operator<< (unsigned short val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (int val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (unsigned int val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (long val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (unsigned long val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (float val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (double val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (long double val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (const void* val) {
    check_lvl(val);
    return oss;
    
}

std::ostream& mss::mlog_oss::operator<< (std::streambuf* sb) {
    check_lvl(sb);
    return oss;
    
}
*/

std::ostream& mss::mlog_oss::operator<< (
        std::ostream& ( *pf )(std::ostream&)) {
    if (!noLog) {
        oss << pf;
    }
    return oss;
    
}
std::ostream& mss::mlog_oss::operator<< (std::ios& ( *pf )(std::ios&)) {
    if (!noLog) {
        oss << pf;
    }
    return oss;
}

std::ostream& mss::mlog_oss::operator<< (
        std::ios_base& ( *pf )(std::ios_base&)) {
    if (!noLog) {
        oss << pf;
    }
    return oss;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out, char c ) {
    if (!out.getNoLog()) {
        out.insert(c);
    }
    return out;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out,
        signed char c ) {
    if (!out.getNoLog()) {
        out.insert(c);
    }
    return out;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out, 
        unsigned char c ) {
    if (!out.getNoLog()) {
        out.insert(c);
    }
    return out;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out, 
        const char * s) {
    if (!out.getNoLog()) {
        out.insert(s);
    }
    return out;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out,
        const signed char * s) {
    if (!out.getNoLog()) {
        out.insert(s);
    }
    return out;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out,
        const unsigned char* s) {
    if (!out.getNoLog()) {
        out.insert(s);
    }
    return out;
}

mss::mlog_oss& mss::operator<< (mss::mlog_oss& out,
        const std::ostream& in) {
    if (!out.getNoLog()) {
        out.insert_os(in);
    }
    return out;
}
