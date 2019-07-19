/* Start of header ********************************/;
/* Program: ur39.sas for m_dart            */;
/********************************* End of header **/;

*** insert GUTS unit testing macros library.  GUTS is Gilead Unit Testing System;
options insert=(sasautos=/biometrics/system_programming/guts/);

*** %include utility, initialize global macro variables, library names;
%guts_init_test;  

*** assert that code review was completed.  Code Review is manually done;
%guts_code_review(ur_id=%str(23-24), result=PASS);

