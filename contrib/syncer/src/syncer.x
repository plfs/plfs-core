struct CopyInfo {
       string from<>;
       string to<>;
       int sync;
};

typedef struct CopyInfo CopyInfo;

program SYNCERPROG {
	version SYNCERVERS {
		int COPY_FILE(CopyInfo) = 1;
	} = 1;
} = 0x20000001;