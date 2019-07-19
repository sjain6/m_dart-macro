/********* start of header ******************************************
Program Name:  m_dart.sas                                             
Author:        Kevin Miller (kmiller05)                                           
Description:   Send dataset to DART staging server to be displayed on DART web portal
Category:      CP
Macros called:
Parameter: 
Usage:         
                                                                       
Change History: 
 2017-06-01 Kevin Miller (kmiller05) Original programmer
 2017-06-07 kmiller05   Updated to create defined erro(r) tables when using fastload to upload to teradata  
 2018-04-05 bliu Updated to standard macro header format 
 2018-07-05 kmiller05 1.Updated to support CPT by adding datacutid and  subjectid to dataset prior to loading.
                          Note: Only when CPT global var = Y.
					  2.add study to dart dup. email     
					  3.Add ISBIOMARKERLISTING to metadata load for non-standard/static listings.
					  4.Add g_testing use, Normal Load (Prod): %let g_testing=0, Dev Load: %let g_testing=1,
					    val load: %let g_testing=2, generate XLSX files in CWD: %let g_testing=3.
 2018-11-28 kmiller05 1.Updated to use SQL server load
                      2.Added new parameter called listtype
					  3.Remove ISBIOMARKERLISTING from metadata load.
					  4.Remove valtest server.
 2018-12-06 kmiller05 1.Update to allow variables with length of 32 chars
                      2.Resolve numeric vars rounding, use best. for non-formated numeric vars
					  3.Allow listcat to override all assumed categories. 
 2019-01-17 bliu      Mask word(s) that lead to logcheck alert message
 2019-05-02 kmiller05 Update email subject line to fix bug, using sysfunc instead of || in warn(ing) masking.
 2019-06-12 mcummings updated merge logic for dart_subjectid relating to CPT, will allow a match solely 
                      on subjid to identify dart_subjectid, code provided by Vinh
********** end of header *******************************************/

%macro m_dart(
	 dsnin     = ,
	 dsnout    = ,
	 listlabel = ,
	 listtitle = ,
	 type      = LISTING,
	 itemurl   = ,
	 keys      = ,
	 cpt_merge_key=invid scrnid subjid,
	 listcat   = ,
	 devtest   =
             );
			 
	 libname b3plib "/biometrics/global/mgarea/data/";
     %let __optmain=%sysfunc(getoption(minoperator));

   options minoperator;
		/** Check if being run in CPT directory, used to load CPT data **/
         %let __scancpt = %sysfunc(pathname(rawdata));
         %if (%index(%upcase(&__scancpt),_CPT)) >0 %then %do;		
		  /** Set prefix to load CPT tables **/
		  %let __prfix=C_;
		  /** Load cut_id from dataset **/
		  proc sql noprint;
           select distinct(CUT_ID) into :__CUT_ID
           from rawdata.vw_dataset_detail
          quit;
		  /** Add in datacutID variable to dataset **/
		  data &dsnin;
		   set &dsnin;
		    datacutid=&__CUT_ID;
		    
		    format datacutid 8.;
		  run; 
		 %end; %else %do;
		   %let __prfix=; 
		   %let __CUT_ID=.;
		 %end;
	 /*** If study is CPT=Y then add in merge vars ***/
       %if %symexist(CPT) %then %do;		 
		%if (%index(%upcase(&CPT),Y)) >0 %then %do;		 
		 /*** Load MERGE Key (subjid) for all listings that are subject level if CPT study = Y ***/
         %if (%length(&cpt_merge_key)>0) %then %do;
 		   /** Load subjectid from dataset **/
			%local __inv __scrn __subj __oth is_miss_subj_val;

			%let __inv=;%let __scrn=;%let __subj=; %let __oth=; %let is_miss_subj_val=1;
			
			%do _aa_=1 %to %sysfunc(countw(&cpt_merge_key));
			  %let curr_i = %upcase(%scan(&cpt_merge_key,&_aa_));
			  %if &curr_i in (SITENUMBER INVID) %then %let __inv=&curr_i;
			  %else %if &curr_i in (SCRNID SCRNNUM) %then %let __scrn=&curr_i;
			  %else %if &curr_i in (SUBJID SUBJECT_ID) %then %let __subj=&curr_i;
			  %else %let __oth=&__oth &curr_i;
			%end; 

  	  /*** if multiple subjects for same id, then delete ***/
     	proc sort data=rawdata.dm out=lu_id;
     		 by invid scrnid subjid;
   		run;

   		data lu_id;
   			  set lu_id;
   			  by invid scrnid subjid;
   			  
   			  if not (first.subjid and last.subjid) then delete;
   		run;

   	  %nobs(&dsnin);	

  	  %if %length(&__subj) ne 0 and &nobs > 0 %then %do;	 
  	  
  	    ods listing close;
  	    proc freq data=&dsnin nlevels;
  	  	  ods output nlevels = nlvl;
  	  	  tables &__subj / noprint;
  	    run; quit;
  	    ods listing;
  	    
  	    proc sql noprint;
  	    	  select nmisslevels into :is_miss_subj_val from nlvl;
  	    quit;
  	  %end;

			proc sql noprint undo_policy=none;
			  create table &dsnin as
			  select d.*, s.subjectid as dart_subjectid format=8.
			  from &dsnin d
			  left join lu_id s
			  on (1=1 
			  
			  	%if %length(&__inv) ne 0 and &is_miss_subj_val ne 0 %then and input(s.invid,best.)=input(d.&__inv,best.);
			  	%if %length(&__scrn) ne 0 and &is_miss_subj_val ne 0 %then %do;
			  		  %if %length(&__subj) ne 0 %then  
  			    	and input(s.scrnid, best.)=input(ifc(not missing(d.&__subj) and missing(d.&__scrn), s.scrnid,d.&__scrn), best.);
  			    	%else and input(s.scrnid, best.)=input(d.&__scrn,best.);
  			  	%end;
			  	%if %length(&__subj) ne 0 %then and input(s.subjid, best.)=input(d.&__subj, best.);
			  	)
			  %if %length(&__subj) ne 0 %then or (not missing(d.&__subj) and input(s.subjid, best.)=input(d.&__subj, best.));
  			  ;
			quit;
			
          %end;
         %end;
        %end;
        /*** Check to see if study level macro var &g_testing exists, if it does use it ***/
        %if %symexist(g_testing) %then %do;		 
		 %if (%length(&devtest)=0) and (&g_testing)=1  %then %do;
		  %let devtest=Y;
		 %end;
		 %if (%length(&devtest)=0) and (&g_testing)=2  %then %do;
		  %let devtest=Y;
		 %end;
		 %if ((%length(&devtest)=0) or (%upcase(&devtest)=UAT)) and (&g_testing)=3  %then %do; 
		  proc export data=&dsnin
		       dbms=xlsx 
               outfile="./&dsnin..xlsx"
			   replace;
          run;
		  %goto notloaded;
		 %end;
		%end;

  /*-----------------------------------------------------------------------------------
    1) Setup DART Teradata SERVER Library and Parameters	  
  -----------------------------------------------------------------------------------*/
      /*** Set local macro vars ***/
      %local __scancpt __optmain __dart1lst __study __timedt __uniqkeys __keycnt __recnum 
	         __time __numobs __numdups __pcpemail __ccemail; 

	  /*** Set name of standard macros (these use method type1 (static listings)) ***/
	  %let __dart1lst = %str(l_aecode l_nonmatch_toxgr l_lab34 l_cmcode l_aesddisc l_aecmmh l_ae34 l_aetox345
	                         l_echecks l_echecks_bm l_echecks_run l_echecks_run_bm l_echecks_com_bm l_echecks_com
							 l_pve_prg l_pve_dth l_pve_sae l_pve_metrics);

	  /*** Set email of add'l resource to be CC'd on duplicate warn||ing emails ***/
	  %let __ccemail=kevin.miller@gilead.com;

      /*** Set DART SQL libnames, Dev, Val and Prod ***/
	/*** Default Libnames ***/
	  /**DART2 PROD  **/ 
	     %m_passw(dartprod);
         libname dart pathname(dartprod);
	%if (%upcase(&devtest)=Y) %then %do;
	     %m_passw(dartdev);
         libname dart pathname(dartdev);
	%end;

       data _null_;
        call symputx('__study',"s"||compress("&sno.","-_/", "aA"));
	   run; 

  %if ("&dsnout "=" ") %then %do;
   %let dsnout=NOTINDART1;
  %end;
 /*** only use type1 if listing namae is for standard dart listings ***/
 %if &dsnout in (&__dart1lst.) %then %do; 

  /*-------------------------------------------------------------------------------
   TYPE 1: STATIC LISTINGS UPLOAD PROCSS HERE:
  -------------------------------------------------------------------------------*/

    /*--------------------------- --------------------------------------------------------
      2) Create DART DATASET and DART METADATA dataset
    -----------------------------------------------------------------------------------*/
	  data _null_;
         tt = strip(compress(put(time(),time.),':'));
         if (length(tt)>4) then tt = substr(tt,length(tt)-3);
         call symput('__time',strip(tt)); 
         call symput('__study',"s"||compress("&sno.","-_/", "aA"));
	   run;

      data __mdatastd;
         length TableName $500 StudyName LISTINGCODE $100 LoadingStatus $50 DataCutId 4.;
         TableName=upcase("&__prfix.&__study._&dsnout._&__time.");
         StudyName=upcase("&__study.");
         LISTINGCODE=upcase("&__prfix.&dsnout.");
         DateTimeLoaded=datetime();
         LoadingStatus='Loaded';
		 DataCutId=&__CUT_ID;

         format datetimeloaded datetime22.3;
         keep tablename studyname LISTINGCODE datetimeloaded loadingstatus DataCutId; 
      run;

	   data _null_;
        call symput('dartdsn',upcase("&__study._&dsnout._&__time."));
		call symput('__mdtatbl',"SAS_STD_DATALOAD");
	   run;
 %end; %else %do;
  /*-------------------------------------------------------------------------------
   TYPE 2: DART 2.0 DYNAMIC UPLOAD PROCESS BEGINS HERE:
  -------------------------------------------------------------------------------*/ 
   /** Check all required parameters are present: ***/
    %if ("&dsnin "=" ") %then %do;
	  %put %sysfunc(cat(ERR,OR:)) Parameter DSNIN is blank: no data sent to DART.;
	  %goto notloaded;
    %end;
    %if ("&listlabel "=" ") %then %do;
	  %put %sysfunc(cat(ERR,OR:)) Parameter LISTLABEL is blank: no data sent to DART.;	
	  %goto notloaded;
    %end;
    %if ("&listtitle "=" ") %then %do;
	  %put %sysfunc(cat(ERR,OR:)) Parameter LISTTITLE is blank: no data sent to DART.;		
	  %goto notloaded;
    %end;
    %if ("&keys "=" ") %then %do;
	  %put %sysfunc(cat(ERR,OR:)) Parameter KEYS is blank: no data sent to DART.;	
	  %goto notloaded;
    %end;

   /** Check to Assign Category ID Metadata Variable  **/
    /** scan for SCRNID SCRNNUM SUBJID SUBJECT_ID **/
	%let CATEGORYID=;
	%let __pcpemail=;
	%if (%index(%upcase(&keys),SCRNID)) >0  or (%index(%upcase(&keys),SCRNNUM)) >0 or (%index(%upcase(&keys),SUBJID)) >0 or (%index(%upcase(&keys),SUBJECT_ID)) >0 %then %do;
	 %let CATEGORYID=2;
	%end; %else %do;
	 %let CATEGORYID=1;
	%end;
    %if (%index(%upcase(&progpath),BMCDP)) >0 %then %do;
	  %let CATEGORYID=%sysevalf(&CATEGORYID.+2);
	  %let __pcpemail=biomarkerCPsupport@gilead.com;
    %end;
	%if %length(&listcat) ne 0 %then %do;
     %let CATEGORYID=&listcat;
    %end;

    /*** Check for duplicates (regardless of case), if duplicates exist, cancel run and email info to primary cp ***/

    data __keydup;
     set &dsnin (keep=&keys);
       array Chars[*] _character_;
        do _dup_ = 1 to dim(Chars);
           Chars[_dup_] = upcase(Chars[_dup_]);
        end;
     drop _dup_;
    run;

	proc sort data = __keydup nodupkey force dupout = __dupchk;
	 by &keys;
	run;

	 %let dsid=%sysfunc(open(__dupchk,i));
	 %let __numdups=%sysfunc(attrn(&dsid,nobs));
	 %let rc=%sysfunc(close(&dsid));

	 %if &__numdups >0 %then %do;
	  %if %length(&__pcpemail) = 0 %then %do;
	   data __pcp;
	    set b3plib.b3pcont;
		 if protocol = "&sno";
		  if pcp = "" then do;
		   call symputx('__pcpemail',strip("&__ccemail"));
		  end; else do;
		   call symputx('__pcpemail',strip(pcp));
		  end;
	   run;
	  %end;
      ods escapechar = "^";
      filename mymail email to=("&__pcpemail") 
	                         cc=("&__ccemail" "&sysuserid.@gilead.com")
	                         from="DART Support <biometrics.support@gilead.com>"
							 sender="DART Support <biometrics.support@gilead.com>"
	                         subject="DART Duplicate Observations %sysfunc(cat(Warn,ing)) &sno"
					    	 type="TEXT/HTML";
       data _null_;
          file mymail;
            put '<html>';
			put '<head>';
			put '<meta content="text/html; charset=ISO-8859-1" http-equiv="content-type">';
			put '<title></title>';
			put '</head>';
			put '<body1>';
			put '<span style="font-size: 12pt;';
			put 'font-family: &quot;Calibri&quot;,&quot;serif&quot;;">';
			put "Attempted upload to DART failed.<br>";
			put "Duplicate records identified, listing not uploaded to DART.<br>";
			put "Update Keys parameter in DART macro call to reflect a unique sort.<br>";
            put '<hr>';
			put '</span>';
			put '</body1>';

            put '<body2>';
			put '<b>';
			put '<span style="font-size: 10pt;';
			put 'font-family: &quot;Calibri&quot;,&quot;serif&quot;;">';
			put '<br>';
            put "Study: &sno<br>";
			put "Program: &progpath/&jobname <br>";
			put "Listing Label: &listlabel <br>";			
			put "Listing Title: &listtitle <br>";
			put "Key Vars: &keys<br>";
			put "Submitted By: &sysuserid<br>";
			put '<b>';
			put '</span>';
			put '</body2>';
			put '</html>';
	   run;
       filename mymail clear;
       %m_message( 6, );
	   %put %sysfunc(cat(WARN,ING:)) Duplicate observations based on keys provided exist in input dataset: data not sent to DART.;
	   %goto notloaded;
	 %end;

       data _null_;
         call symput('__timedt',strip(put(today(),best.))||strip(compress(put(timepart(datetime()),time12.3),':.')));
		 call symput('__mdtatbl',"SAS_NONSTD_DATALOAD");
		 call symputx('__keycnt',countw(COMPBL("&keys")));
		 call symputx('__uniqkeys',upcase("&keys"));
	   run;

      /** Check #obs in dataset**/
       %let dsid=%sysfunc(open(&dsnin,i));
       %let __numobs=%sysfunc(attrn(&dsid,nobs));
       %let rc=%sysfunc(close(&dsid));	

	   /*** Set Keys into macro var for metadata sorting on DART side ***/

	   data __keyvrsmv;
	    length name $200 keys $4000;
	    keys=tranwrd(trim(COMPBL("&keys")),' ',', ');
		%do _key_=1 %to &__keycnt;
		 name=upcase(scan(keys,&_key_,', '));
		 output;
		%end;
		drop keys;
	   run;

	   proc sort data = __keyvrsmv;
	    by name;
	   run;

       /*** Create Metadata Table ***/
  	    data __mdatanonstd;
         length STUDYNAME  LISTINGCODE  $100 LISTINGTITLE $500 ITEMTYPE_TD $100 ORDERBYCOL $800 ITEMURL_TD $2000
		        LST_STRUCT_TABLENAME LST_BODY_TABLENAME $100 LOADINGSTATUS $50 DataCutId 4.;

		  STUDYNAME=upcase("&__study.");
		  LISTINGCODE ="&listlabel";
		  LISTINGTITLE="&listtitle";
	      ITEMTYPE_TD="Listing";
		  ITEMURL_TD="";
		  LST_STRUCT_TABLENAME="&__prfix.LST_STRCT_&__timedt";
		  LST_BODY_TABLENAME="&__prfix.LST_BODY_&__timedt";
	      DATETIMELOADED=datetime();
          LOADINGSTATUS='Loaded';
		  ORDERBYCOL=tranwrd(trim(COMPBL("&keys")),' ',', ');
		  CATEGORYID=&CATEGORYID;
		  DataCutId=&__CUT_ID;

		  format DATETIMELOADED datetime22.;
		run;

	    /*** Create Listing Structure Variable Metadata Table ***/
	       %local __dslib __dsname;
	       %if %index(&dsnin,.) %then %do;
	       	    %let __dslib=%scan(&dsnin,1);
	       	    %let __dsname=%scan(&dsnin,2);
	       %end;
	       %else %do;
	       	    %let __dslib=WORK;
	       	    %let __dsname=&dsnin;
	       %end;

         proc sql noprint;
           create table __dsnincont as
           select distinct memname, name, label, type, varnum,  put(length, best.) as length_char, format
            from sashelp.vcolumn
              where libname = upcase("&__dslib") and
                    memname = upcase("&__dsname")
             order by name;
         quit;

		 data __lstruct;
		  length name $200;
		  set __dsnincont;
		     if (format="") then do;
			  if upcase(TYPE) = "CHAR" then do;
               fmtcode = "$"||strip(length_char);
			  end; else do;
			   fmtcode = "best.";
			  end;
             end; else do;
               fmtcode = strip(format);
             end;
			 name=upcase(name);
			 keep varnum name label fmtcode type;
		run;

	   proc sort data = __lstruct;
	    by name;
	   run;

		data __lstruct2 (keep=columnvar_td columnvar_sql columnname_td columnorder_td columnformat_td iskeyvariable type);
		 length columnvar_td columnname_td  columnformat_td $75 columnorder_td 8. iskeyvariable $1;
		 merge __lstruct(in=_a_) __keyvrsmv(in=_b_);
		  by name;
		   if _a_ and _b_ then iskeyvariable="Y";
		   else iskeyvariable="N";
		   columnvar_td="S_"||strip(substr(name,1,30));
		   columnvar_sql=strip(substr(name,1,32));
		   /** Remove teradata specific special chars from column name **/
		   columnname_td=strip(compress(label,".,'/;[]!@#$%^&*()_{}|\?><:"));
		   columnorder_td=varnum;
		   columnformat_td=strip(fmtcode);
		run;

	   options missing=' ';	
	   /*** Create Listing Body Table ***/

         proc sort data=__lstruct2;
           by columnorder_td;
         run;

         data _null_;
           set __lstruct2;
              call symput( '__vnam'||trim(left(put(_N_,best.))), trim(columnvar_sql));
              call symput( '__newvnam'||trim(left(put(_N_,best.))), trim(substr(columnvar_td,1,32)));
              call symput( '__dart_XX_'||trim(left(put(_N_,best.))),'__dart_XX_'||trim(left(put(_N_,best.))));	
              call symput( '__vtyp'||trim(left(put(_N_,best.))),strip(upcase(substr(type,1,1))));
              call symput( '__vfmt'||trim(left(put(_N_,best.))), trim(columnformat_td));
              call symput( '__vnum',trim(left(put(_N_,best.))));
         run;

	     data __lbody;
	      set &dsnin;
	        %do _ii_ = 1 %to &__vnum;
			   %if (&&__vtyp&_ii_=C) %then %do;
                __dart_XX_&_ii_ = &&__vnam&_ii_;
               %end; %else %do;
                __dart_XX_&_ii_ = left(put(&&__vnam&_ii_, &&__vfmt&_ii_));
               %end;
	        %end;
		  keep __dart_XX_:;
	     run;

		 data __lbody2;
		  set __lbody;
		  %do _ii_ = 1 %to &__vnum;
		   &&__newvnam&_ii_ = __dart_XX_&_ii_;
		  %end;
		  drop __dart_XX_:;
		 run;
         Options Missing=.;

    /*-----------------------------------------------------------------------------------
      3) Assign Tables to DART SQL SERVER
    -----------------------------------------------------------------------------------*/
    %end;

     proc sql;
	   %if &dsnout in (&__dart1lst.) %then %do; 
            create table dart.&__prfix.&dartdsn  as
               select * from &dsnin;	   
            insert into dart.SAS_STD_DATALOAD (tablename, studyname, LISTINGCODE, datetimeloaded, loadingstatus, DataCutId)
               select tablename, studyname, LISTINGCODE, datetimeloaded, loadingstatus, DataCutId
			from __mdatastd;
	   %end; %else %do;   
            create table dart.&__prfix.LST_STRCT_&__timedt  as
               select * from __lstruct2;
            create table dart.&__prfix.LST_BODY_&__timedt as
               select * from __lbody2;	   
            insert into dart.SAS_NONSTD_DATALOAD (STUDYNAME, LISTINGCODE , LISTINGTITLE, ITEMTYPE_TD, ITEMURL_TD,
			             LST_STRUCT_TABLENAME, LST_BODY_TABLENAME, DATETIMELOADED, LOADINGSTATUS, 
						 CATEGORYID, ORDERBYCOL, DataCutId)
               select STUDYNAME, LISTINGCODE , LISTINGTITLE, ITEMTYPE_TD, ITEMURL_TD, LST_STRUCT_TABLENAME, LST_BODY_TABLENAME,
			     DATETIMELOADED, LOADINGSTATUS, CATEGORYID, ORDERBYCOL, DataCutId
            from __mdatanonstd;
	  %end;
      quit;

	  %notloaded:

   options &__optmain;

%mend m_dart;
