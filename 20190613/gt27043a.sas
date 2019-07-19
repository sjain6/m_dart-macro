/* Start of header ********************************/;
/*  Program: gt27043a.sas                         */;
/********************************* End of header **/;


*** insert GUTS unit testing macros library. ;
options insert=(sasautos=/biometrics/system_programming/guts);

%guts_init_test;

%include "/biometrics/global/mgarea/utilities/macros/header_check_gt27043a.sas";
%header_check_gt27043a (path=&CURRPATH, prg=&UTILITY);

%macro doit;
	**Only print gt23043a form when headercheck has 0 obs;
%nobs(here.headercheck);
%if &nobs=0 %then %do;
	%guts_driver(guts_utility=&utility, guts_folderdt=&progdate);
%end;

%mend doit;
%doit;
