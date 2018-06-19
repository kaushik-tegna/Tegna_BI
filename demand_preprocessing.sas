/* Hard coded start of the year dates. I think its better to have all of the hardcoding in one ugly place . */
log="/teg/warehouseA/default/joshlog.log"; 
%let year_start_2010 = '04jan2010'd;
%let year_start_2011 = '03jan2011'd;
%let year_start_2012 = '02jan2012'd;
%let year_start_2013 = '01jan2013'd;
%let year_start_2014 = '01jan2014'd;
%let year_start_2015 = '01jan2015'd;
%let year_start_2016 = '04jan2016'd;
%let year_start_2017 = '02jan2017'd;
%let year_start_2018 = '01jan2018'd;
%let year_start_2019 = '31dec2018'd;

data tegdata.all_stations_cum_sum_pre;
	set sqlprod.BUYER_DEMAND_FCST_INPUT_new2(rename=(daypart_name=day_part station_name=station invcode_name=inventory_code full_date=air_date));
	format ordered_week date9. air_date date9.;
	where gross_revenue >0;
	if year(ordered_week) = 2006 then delete;
	air_week = WEEK(air_date, 'v');
	air_year =YEAR(air_date-WEEKDAY(air_date)+5);
	ordered_week = datepart(order_created);
	if air_week = 53 then delete;
	date_difference = air_date-ordered_week;
run;

proc sql;
create table count as
select station, day_part,  inventory_code, air_week, air_year, count(*) as order_counts from tegdata.all_stations_cum_sum_pre
where air_year < 2018
group by station, day_part, inventory_code, air_week, air_year;
run;

proc sort data=tegdata.all_stations_cum_sum_pre;
	by market  station day_part inventory_code air_week air_year ordered_week;
run;

data tegdata.all_demand_fixed;
	set tegdata.all_stations_cum_sum_pre;
	by market  station day_part inventory_code air_week air_year ordered_week;
	if date_difference > 365 then do;
		if air_year = 2010 then ordered_week = &year_start_2010;
		if air_year = 2011 then ordered_week = &year_start_2011;
		if air_year = 2012 then ordered_week = &year_start_2012;
		if air_year = 2013 then ordered_week = &year_start_2013;
		if air_year = 2014 then ordered_week = &year_start_2014;
		if air_year = 2015 then ordered_week = &year_start_2015;
		if air_year = 2016 then ordered_week = &year_start_2016;
		if air_year = 2017 then ordered_week = &year_start_2017;
		if air_year = 2018 then ordered_week = &year_start_2018;
	end; 
run;

proc sort data=tegdata.all_demand_fixed;
	by market  station day_part inventory_code air_week air_year ordered_week;
run;

proc sql;
	create table tegdata.capacity_key as
	select distinct station_name, invcode_name, max(air_year) as max_year, avg(round(potential_units, 0.5)) as capacity, air_week, daypart_name
	from sqlprod.buyer_demand_fcst_input_new2 where air_year in (2017, 2018)
	group by station_name, invcode_name, air_week, daypart_name, air_year;
quit;

data tegdata.all_stations_cum_sum;
	set tegdata.all_demand_fixed;
	format air_date date9. ordered_week date9.;
	by market  station day_part inventory_code air_week air_year ordered_week; 
	 /*	if intck("DAY",ordered_week,air_date) >365 then ordered_week = datdif(364,air_date,"ACT/ACT"); */
	run;

proc sort data=tegdata.all_stations_cum_sum;
	by market  station day_part inventory_code air_week air_year ordered_week; 
run;

/* putting in the end statement does it for all by groups. you cant extend the end just for one group. */
/* Youre changing the dependent variable from cum_sum to spot counts. The idea was to make a time series out of the spot counts, and THEN do a cumulative sum. */
proc timeseries data=tegdata.all_stations_cum_sum out=tegdata.all_stations_time_series_1 SEASONALITY= 52; 
      by market  station day_part inventory_code air_week air_year; 
      id ordered_week interval=week1.2 accumulate=total  SETMISSING = missing;
      var spot_counts;
   run;

proc sort data=tegdata.all_stations_time_series_1;
	by station day_part inventory_code air_week air_year ;
run;

proc sort data=tegdata.all_stations_time_series_1;
 /* you recently added ordered_week to this sort . */
	by market  station day_part inventory_code air_week air_year ; 
run;

data tegdata.assign_air_date;
	set tegdata.all_stations_time_series_1;
	format air_date date9.;
		if (air_year =2011) then air_date = intnx('week', &year_start_2011, air_week,'S');
		if (air_year =2012) then air_date = intnx('week', &year_start_2012, air_week,'S');
		if (air_year =2013) then air_date = intnx('week', &year_start_2013, air_week,'S');
		if (air_year =2014) then air_date = intnx('week', &year_start_2014, air_week,'S');
		if (air_year =2015) then air_date = intnx('week', &year_start_2015, air_week,'S');
		if (air_year =2016) then air_date = intnx('week', &year_start_2016, air_week,'S');
		if (air_year =2017) then air_date = intnx('week', &year_start_2017, air_week,'S');
		if (air_year =2018) then air_date = intnx('week', &year_start_2018, air_week,'S');
run;

data tegdata.all_stations_time_series_2;
	set tegdata.assign_air_date;
	by market  station day_part inventory_code air_week air_year; 
	if first.air_year then cum_sum_2 = 0 ;
	cum_sum_2 + spot_counts;
run;

proc sort data=tegdata.all_stations_time_series_2;
	by station day_part inventory_code air_week air_year ordered_week;
run;

data tegdata.historical_years_extensions;
	set tegdata.all_stations_time_series_2;
	by station day_part inventory_code air_week air_year ;
	format air_date date9.;
	if last.air_year and week(ordered_week) ^= week(air_date) then do;
		ordered_week = air_date;
		is_artifical=1;
		output tegdata.historical_years_extensions;
		end;
		run;

data tegdata.extended_time_series;
	set tegdata.all_stations_time_series_2 tegdata.historical_years_extensions;
	run;

proc sort data=tegdata.extended_time_series;
	by market station day_part inventory_code air_week air_year ordered_week;
run;

proc timeseries data=tegdata.extended_time_series out=tegdata.all_stations_time_series_3 SEASONALITY= 52;
      by market station day_part inventory_code air_week air_year; 

      id ordered_week interval=week1.2 accumulate=total   SETMISSING = previous;
      var cum_sum_2;
run;

proc sort data=tegdata.all_stations_time_series_3;
 	by market station day_part inventory_code air_week ordered_week ;
run;


proc timeseries data=tegdata.all_stations_time_series_3
                   out=tegdata.all_stations_time_series_4 SEASONALITY= 52;
      by market station day_part inventory_code air_week ; 
	  /* put the end variable into a global, so then you can just change it at the top or something */
		/* CHANGE START BACK TO start='03jan2014'd */
      id ordered_week interval=week1.2 accumulate=total   SETMISSING = missing end="&sysdate"d;
      var cum_sum_2;
   run;

data tegdata.all_stations_time_series_5;
	set tegdata.all_stations_time_series_4 end=eof;
	by market station day_part inventory_code air_week;
	format air_date date9.;
	/* fill in missing values where the air_year isnt this year or this year minus 1 */
	current_year = year(Today());
	
	 if week(Today()) > air_week then air_date = intnx('week', &year_start_2019, air_week,'S');
	 if week(Today()) < air_week then air_date = intnx('week', &year_start_2018, air_week,'S');
   	if missing(cum_sum_2) and year(ordered_week) <= current_year - 1 then do;
	cum_sum_2 = 0;
	is_early_year = 1;
	end;
	if missing(cum_sum_2) and air_week in (51, 52)  and year(ordered_week) = current_year then cum_sum_2 = 0;



	if missing(cum_sum_2) and week(ordered_week) <= week(Today())  and air_week < week(Today()) then do; 
	cum_sum_2 = 0;
	past_air_week = 1;
	end;

	/*if missing(cum_sum_2) and week(ordered_week) >= week(Today())  and month(ordered_week) = month(today()) then do; */
	if missing(cum_sum_2)   and air_week >= week(Today()) and week(ordered_week) <= week(Today())  then do;
	future_air_week = 1;
	retain _cum_sum_2;
 	if not missing(cum_sum_2) then _cum_sum_2=cum_sum_2;
   	else cum_sum_2=_cum_sum_2;
  	drop _cum_sum_2;
	end; 

	retain _cum_sum_2;
 	if not missing(cum_sum_2) then _cum_sum_2=cum_sum_2;
   	else cum_sum_2=_cum_sum_2;
  	drop _cum_sum_2; 
	
	if past_air_week = . then past_air_week = 0;
	if future_air_week = . then future_air_week = 0;
	if is_early_year = . then is_early_year  = 0;
	if ordered_week <Today() and cum_sum_2 > 0   then flag = 1;
	else flag = 0;
	output;
	if last.air_week; 
		do i = 1 to 52;
		date_difference = week(air_date) - week(ordered_week);
		if date_difference > 0  then flag = 1;
		else flag = 0;
		cum_sum_2=.;
		ordered_week = intnx('week1.2', ordered_week,1, 'S');
		station = station;
		day_part = day_part;
		inventory_code = inventory_code;
		output;
		end;
		 

run;

proc sort data=tegdata.all_stations_time_series_5;
	by market station day_part Inventory_code air_week;
run;

/* Create the input comparison table */
proc sql;
create table tegdata.input_collection as
select distinct station, day_part, inventory_code, air_week
from tegdata.all_stations_time_series_5;
quit;

/*---------------------------EVERYTHING UP IS MAKES THE 15 Minute ratings set up properly--------------------------*/
proc hpfidmspec repository=work.rep
                name=idm
                label="Automatically Selected Best IDM Model";
   idm interval=( method=simple transform=auto criterion=mape )
       size    =( method=simple transform=auto criterion=mape )
       average =( method=simple transform=auto criterion=mape );
run;

proc hpfesmspec rep=work.rep specname=bests;
esm method=bests;
run;

proc hpfesmspec rep=work.rep specname=bestn;
esm method=bestn;
run;

proc hpfesmspec rep=work.rep specname=best;
esm method=best;
run;

proc hpfarimaspec rep=work.rep specname=airline;
forecast symbol=y dif=(1,s) q=(1)(1)s noint ;
run;

proc hpfselect rep=work.rep name=combavgs label= "AICC AVERAGE";
combine method=average;
spec airline bests;
run;

proc hpfselect rep=work.rep name=combavg label= "AICC AVERAGE";
combine method=average;
spec airline best;
run;

proc hpfselect rep=work.rep name=combavgn label= "AICC AVERAGE";
combine method=average;
spec airline bestn;
run;

proc hpfselect rep=work.rep name=combavgsaicc label= "AICC AVERAGE";
combine method=aicc;
spec airline bests;
run;

proc hpfselect rep=work.rep name=combavgaicc label= "AICC AVERAGE";
combine method=aicc;
spec airline best;
run;

proc hpfselect rep=work.rep name=combavgnaicc label= "AICC AVERAGE";
combine method=average;
spec airline bestn;
run;

proc hpfselect rep=work.rep name=airselect;
select  criterion=rmse holdout=52;
spec airline idm  bests combavg combavgn combavgs combavgaicc combavgnaicc combavgsaicc;
run;

proc hpfengine data=tegdata.all_stations_time_series_5
rep=work.rep globalselection=airselect outfor=tegdata.forecast_fit_results_two outmodelinfo=work.model_selection_list lead=52 task=select(minobs=25);
by market station day_part Inventory_code air_week;
id ordered_week interval=week1.2;
forecast cum_sum_2;
adjust cum_sum_2=(flag) / operation=(DIVIDE, multiply ); 
run;

proc sql;
create table failed_models as
select * from model_selection_list where _MODEL_ = '_MEAN_';
quit;

data tegdata.demand_update;
	set tegdata.forecast_fit_results_two;
	by market station day_part inventory_code air_week;
	lag_demand = lag(actual);
	 /* if year(ordered_week) = 2017 and ordered_week = "25SEP17"d and predict < lag_demand then is_broken = 1; */
	if predict < 0 then predict = 0;
	if ordered_week < "&sysdate"d then delete;
	run;

/* create the output comparison table */
proc sql;
create table tegdata.output_comparison as
select distinct station, day_part, inventory_code, air_week
from tegdata.demand_update;
quit;

/* Now I can compare the input and output datasets to see if anything dropped off */
proc compare base=tegdata.input_collection compare=tegdata.output_comparison;
Title1 'Comparing forecast input to forecast output';
quit;

proc compare base=tegdata.input_collection compare=tegdata.output_comparison;
Title1 'Comparing forecast input to forecast output';
ods pdf file="/teg/warehouseA/default/compare_results.pdf"; 
quit;
%let email_address = ("wdane@tegna.com" "agadde@tegna.com" "avijayagop@tegna.com" "wdane@tegna.com" "jdebellis@tegna.com");
/* email the results to the bi support address */
options emailsys=smtp emailhost=relay.tgna.tegna.com emailport=25;
/* The FILENAME statement dictates who receives the emails */
FILENAME outbox EMAIL &email_address
 Subject='Test Mail message' attach="/teg/warehouseA/default/compare_results.pdf";
DATA _NULL_;
FILE outbox;
FROM = ("noreply@tegna.com");
PUT "Hello";
PUT "This is a test of the forecasting check";
RUN; 

data khou;
set tegdata.forecast_fit_results;
where station = 'KHOU' and inventory_code = 'CBS Early Morning News' and air_week = 34;
run;

data jeopardy;
set tegdata.forecast_fit_results;
where station = 'KPNX' and inventory_code = 'Sun Prime A' and air_week= 33;
run;