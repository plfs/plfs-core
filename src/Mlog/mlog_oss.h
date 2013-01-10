#ifndef MLOG_OSS_H
#define MLOG_OSS_H

#include <iostream>
#include <fstream>
#include <sstream>
#include "mlog.h"

namespace mss {
    class mlog_oss:public std::ostream {
        private:
            std::ostringstream oss;
            int level;
            bool noLog;

        public:
            mlog_oss();
            mlog_oss(int lvl);
            bool getNoLog();
            
            template <typename T>
            void insert(T xin) {
                oss << xin;
            } 
            
            void insert_os(const std::ostream& in);

            std::string str();

            template <typename T>
            std::ostream& operator<< (T val) {
                if (!noLog) {
                    oss << val;
                }
                return oss;
            }

            /*
            std::ostream& operator<< (bool val);
            std::ostream& operator<< (short val);
            std::ostream& operator<< (unsigned short val);
            std::ostream& operator<< (int val);
            std::ostream& operator<< (unsigned int val);
            std::ostream& operator<< (long val);
            std::ostream& operator<< (unsigned long val);
            std::ostream& operator<< (float val);
            std::ostream& operator<< (double val);
            std::ostream& operator<< (long double val);
            std::ostream& operator<< (const void* val);
            std::ostream& operator<< (std::streambuf* sb);
            */

            std::ostream& operator<< (std::ostream& ( *pf )(std::ostream&));
            std::ostream& operator<< (std::ios& ( *pf )(std::ios&));
            std::ostream& operator<< (std::ios_base& ( *pf )(std::ios_base&));

            friend mlog_oss& operator<< (mlog_oss& out,
                    char c);
            friend mlog_oss& operator<< (mlog_oss& out,
                    signed char c);
            friend mlog_oss& operator<< (mlog_oss& out,
                    unsigned char c);
            friend mlog_oss& operator<< (mlog_oss& out,
                    const char* s);
            friend mlog_oss& operator<< (mlog_oss& out,
                    const signed char* s);
            friend mlog_oss& operator<< (mlog_oss& out,
                    const unsigned char* s);
            friend mlog_oss& operator<< (mlog_oss& out,
                    const std::ostream& os);
    };

}
#endif
