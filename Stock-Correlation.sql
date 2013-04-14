/* 
  
  The following is a PL/SQL oracle script to find the correlation between
    different stocks that trade at least an average of 10 million dollars 
    a day and also gives dividends.

  companies_10_mil_plus_div table contains: 
        1) stocks that average 10 million dollars a day trading
        2) stocks that give dividends
        3) data is taken from yahoo finance and spans 1962 to 2013

  tdate_symbol_percent_top_symb table contains:
        1) daily gain/loss % for each stock in companies_10_mil_plus_div 
           for every day from 1962 to 2013
        2) data is taken from yahoo finance and spans 1962 to 2013

  stock_pairs table contains:
        1) correlation number between all stock combinations from 
           companies_10_mil_plus_div table

*/



Declare 
  
  var_symbol varchar2(55);
  i integer;
  h integer;
  var_x_symbol varchar2(55);
  var_y_symbol varchar2(55);
  stock_correlation number;
   
   type cursor_variable is ref cursor;
   v_myCursor cursor_variable;
   v_stocks_corr stock_pairs%ROWTYPE;
   
Begin
 
   i := 1;
  select count(*) into h from companies_10_mil_plus_div;

  loop
   
     select symbol into var_symbol 
      from (
            select symbol 
       	      from (
          		      select symbol, rownum as rn 
          			      from ( 
                		        select symbol 
                              from companies_10_mil_plus_div 
                              order by symbol
     			                  )
                   ) 
            where rn = i
            );

      open v_myCursor for 
        select e.x_symbol, e.y_symbol, (case when x2_variance*y2_variance <= 0 then 0 else   
          (xy_covariance/sqrt(x2_variance*y2_variance)) end) correlation 
          from ( 
                select d.x_symbol, d.y_symbol, d.x_date, (x2_avg-power(x_prcnt_avg,2)) as  
                  x2_variance, (y2_avg-power(y_prcnt_avg,2)) as y2_variance, 
                  (xy_avg-(x_prcnt_avg*y_prcnt_avg)) as xy_covariance 
                  from ( 
                        select c.x_date, c.x_symbol, c.y_symbol, 
                          sum(xy) over (partition by y_symbol) as xy_sum, 
                          sum(x2) over (partition by y_symbol) as x2_sum, 
                          sum(y2) over (partition by y_symbol) as y2_sum, 
                          avg(x2) over (partition by y_symbol) as x2_avg, 
                          avg(y2) over (partition by y_symbol) as y2_avg, 
                          avg(xy) over (partition by y_symbol) as xy_avg, 
                          avg(x_prcnt) over (partition by y_symbol) as x_prcnt_avg, 
                          avg(y_prcnt) over (partition by y_symbol) as y_prcnt_avg 
                          from ( 
                                select a.tdate as x_date, a.symbol as x_symbol, a.prcnt as x_prcnt,
                                  b.symbol as y_symbol, b.prcnt as y_prcnt, (a.prcnt*b.prcnt) as  
                                  xy, power(a.prcnt,2) as x2, power(b.prcnt,2) as y2 
                                  from tdate_symbol_percent_top_symb a, tdate_symbol_percent_top_symb b 
                                  where a.tdate < '04-feb-2013' and a.tdate = b.tdate and  
                                  a.symbol = var_symbol order by a.symbol asc
 			                          ) c 
                        ) d 
                ) e
          where x_date = '01-feb-2013';

      loop
     
         fetch v_myCursor into var_x_symbol, var_y_symbol, stock_correlation;
         exit when v_myCursor%NOTFOUND;
         insert into stock_pairs values (var_x_symbol, var_y_symbol, stock_correlation);
         
      end loop;
    
      i := i+1;
      exit when i > h;
              
  end loop;
    
  close v_myCursor;
     
end;
