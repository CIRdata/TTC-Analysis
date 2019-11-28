
--##############################################
--################PART 2########################
--######Route Data Processing###################
--##############################################

/*
calc_stop_paths:
    - In this section we create a series of lines which represent the path between each of the stops on the route/direction
*/

drop table if exists calc_stop_paths cascade;

--tranlation the lat/lon points into a geom field
with g as (
select pathid, oid, routetag, 
    st_setsrid(st_makepoint(lon::double precision, lat::double precision), 4326) as geom
from tbl_ttc_route_path
order by routetag, oid
), 

--takes the geom field from g and joins those points into a line for each routetag/path in order oid (which is the order that
-- the points exist in in the XML file
p as (
select g.routetag, g.pathid, st_makeline(g.geom order by g.oid) as path_geom
from g
group by g.routetag, g.pathid
), 

--takes the stop lat/lon pionts and translate to geom field
s as (
select ds.oid,  ds.tag, ds.directiontag, ds.routetag, 
    st_setsrid(st_makepoint(rs.lon::double precision, rs.lat::double precision), 4326) as stop_geom
from tbl_ttc_dir_stop ds
inner join tbl_ttc_route_stop rs on rs.tag = ds.tag and ds.routetag = rs.routetag
), 

-- from s we want the previous stop geom feild on the same line as the current stop we are looking at.  The
-- the previous stop --> current stop describes the start and end points of the line between one stop and the next
s_geom as (
select s.oid, s.tag, s.directiontag, s.routetag as routetags, s.stop_geom,
lag(s.stop_geom) over (partition by s.routetag, s.directiontag order by s.oid) as prev_stop_geom
from s
), 

-- here we find the distance from each of the paths (from subqry p above) to the stop/prev_stop positions.  We will
-- later use those distaces to select the best path for each route segment
res as (
select sg.oid, sg.tag, sg.directiontag, sg.routetags, sg.stop_geom, sg.prev_stop_geom, p.routetag, p.pathid,
    p.path_geom, st_distance(sg.stop_geom::geography, p.path_geom::geography) as st_dist,
    st_distance(sg.prev_stop_geom::geography, p.path_geom::geography) as end_dist
from s_geom sg
inner join p on p.routetag = sg.routetags
where sg.prev_stop_geom is not null
order by sg.oid
), 

-- here we locate each stop/prev_stop postion on the closes location on each path from the res subqry
pos as (
select r.oid, r.tag, r.directiontag, r.routetags, r.stop_geom, r.prev_stop_geom, r.routetag, r.pathid, r.path_geom, r.st_dist, r.end_dist, 
    case when st_linelocatepoint(r.path_geom, r.stop_geom) <= st_linelocatepoint(r.path_geom, r.prev_stop_geom) 
        then st_linelocatepoint(r.path_geom, r.prev_stop_geom)
        else st_linelocatepoint(r.path_geom, r.stop_geom) end as end_point_pos,
    case when st_linelocatepoint(r.path_geom, r.stop_geom) > st_linelocatepoint(r.path_geom, r.prev_stop_geom) 
        then st_linelocatepoint(r.path_geom, r.prev_stop_geom)
        else st_linelocatepoint(r.path_geom, r.stop_geom) end as start_point_pos
from res r
), 

-- filter the pos table so that we are left only path and stop postions which have a distance of < 1meter
posflt as (
select pos.oid, pos.tag, pos.directiontag, pos.routetag, pos.stop_geom, pos.prev_stop_geom,
st_linesubstring(pos.path_geom, pos.start_point_pos, pos.end_point_pos) as stop_path_geom,
st_length(st_linesubstring(pos.path_geom, pos.start_point_pos, pos.end_point_pos)) as stop_path_len
from pos
where pos.st_dist < 1::double precision and pos.end_dist < 1::double precision
), 

-- there are some cases where there are multiple paths that have the start/end positions less than 1 meter from the path
-- in those cases we select the shortest of those paths
dd as (
select pf.oid, pf.tag, pf.directiontag, pf.routetag, pf.stop_geom, pf.prev_stop_geom, pf.stop_path_geom, pf.stop_path_len,
row_number() over (partition by pf.oid, pf.tag, pf.directiontag, pf.routetag order by pf.stop_path_len) as rn
from posflt pf
)

/*
oid - order of the stops for each directiontag
tag - unique identifier for each stop
routetag - route eg 501 for the Queen Streetcar service
directiontag - this is the unique identifier route/direction.  Eg, the 501 route, would have separate paths for each of direction
          it travels in as well as a separate identifier if there are any variants of the 501 route
stop_geom/prev_stop_geom - start/end points of path between two stops
stop_path_geom - the line between two stops
stop_path_len          
*/
          
select dd.oid, dd.tag, dd.directiontag, dd.routetag, dd.stop_geom, dd.prev_stop_geom, dd.stop_path_geom, dd.stop_path_len
into calc_stop_paths
from dd
where dd.rn = 1;

          
          
          


/*
calc_direction_paths
- concatinate all of the lines between stops for each route direction into one long line that describes the whole route direction
- note, we need to order the stops (by order in XML file ie the oid) so that they get concatinated in the correct order
*/

drop table if exists calc_direction_paths cascade;

select directiontag, routetag,st_makeline(stop_path_geom order by oid) path_geom
into calc_direction_paths
from calc_stop_paths
group by directiontag, routetag;


create index inx_calc_direction_paths_directiontag on calc_direction_paths (directiontag);
          
          
--##############################################
--################PART 3########################
--#########Location Data Processing#############
--##############################################
          

-- we will filter out some data points from the analysis if we think they look erronious
drop table if exists calc_location_removes cascade;
create table calc_location_removes (id text, remove_reason text, analysis_time bigint);

/*
calc_location_log_01
- sets the srid to 4326 which is the id of WGS 84 which is the spacial system used by GPS navigation
- calculates the actual time of the postion report = time we recieved the report - how many seconds old the report was when recieved
*/

drop table if exists calc_location_log_01 cascade;
select dirtag, id, st_setsrid(st_makepoint(lon::float, lat::float), 4326) geom_position, routetag,
timestamp::bigint/1000 - secssincereport::bigint actual_time
into calc_location_log_01
from tbl_ttc_location_log;

create index inx_calc_location_log_01_id on calc_location_log_01 (id);
create index inx_calc_location_log_01_actualtime on calc_location_log_01 (actual_time);

/*
analysis_times
- the vehicles report on a regular basis but the time periods between reports do have some veriation in them,
for our analysis we want to set a standardized time period every 30sec, this will make compiling results across
vehicles easier
*/

drop table if exists analysis_times cascade;
with a as (
select min(actual_time) - min(actual_time) % 30 start_time, (max(actual_time) - min(actual_time))/30 analysis_periods
from calc_location_log_01
)

select start_time+i*30 analysis_time, extract('hour' from fn_epoch_to_dt(start_time+i*30)) hr, fn_epoch_to_dt(start_time+i*30)::date dt, fn_epoch_to_dt(start_time+i*30) dt_time
into analysis_times
from generate_series (0,(select analysis_periods from a)) s(i) 
cross join a;

create unique index inx_analysis_times_actualtime on analysis_times (analysis_time);

/*
calc_location_log_02
- for each analysis time point we want to find the reported time point before and after the analysis time point,
it is from the before/after positions of the vehicles that we will calculate the postion of the vehicle at the 
analysis time
- we only look 600 secs before and after the analysis times for available reports both because we don't want to be
interpolating reports over too long of a period as well as managing query performance

*/


drop table if exists calc_location_log_02 cascade;
with p as (
select prev.id, t.analysis_time, max(prev.actual_time) prev_report_time
from calc_location_log_01 prev
inner join analysis_times t
    on prev.actual_time <= t.analysis_time and  prev.actual_time >= (t.analysis_time - 600)
group by prev.id, t.analysis_time),

a as (
select aft.id, t.analysis_time, min(aft.actual_time) aft_report_time
from calc_location_log_01 aft
inner join analysis_times t
    on aft.actual_time > t.analysis_time and aft.actual_time <= (t.analysis_time + 600)
group by aft.id, t.analysis_time)

select a.*, p.prev_report_time
into calc_location_log_02
from p
inner join a on a.id = p.id and a.analysis_time = p.analysis_time;

/*
calc_location_log_03
- we are looking up the position of the vehicle at the before and after report times
- if there is a change in direction we will take the ending position as the starting position (so both ending
and starting postitions are the same) this is because of how we calculate the path the vehicle has taken over the analysis 
time period.  We look at the percentage of the route (from start to finish) the vehicle had covered at the start of the 
period vs at the end of the period BUT when the bus is comming up to the end of the route the prev percent finished might 
be 99% (because it's comming up to the end of route) and the after percent finished might be 1% because it's just
starting the return direction
*/

drop materialized view if exists calc_location_log_03 cascade;
create materialized view calc_location_log_03 as 
select rt.*, prev.dirtag, prev.routetag, 
    case when prev.dirtag <> aft.dirtag then aft.geom_position else prev.geom_position end prev_geom_position, 
    aft.geom_position aft_geom_position
from calc_location_log_02 rt
inner join calc_location_log_01 prev
    on prev.id = rt.id and prev.actual_time = rt.prev_report_time
inner join calc_location_log_01 aft
    on aft.id = rt.id and aft.actual_time = rt.aft_report_time
left join calc_location_removes rm on rm.id = rt.id and rm.analysis_time = rt.analysis_time
where rm.id is null;

create index inx_calc_location_log_03_dirtag on calc_location_log_03 (dirtag);
create index inx_calc_location_log_03_id on calc_location_log_03 (id);


/*
calc_location_log_04
- within the vehicle location files we have direction tags that do not match to any route descriptions in the
route description XML.  
- We will try to pick the best route description for those un-matched direction tags by first looking for a matching
route direction (note direction tags follow the format <route_direction_variant> eg 501_1_501A means it's the 501 route
the '1' direction and the 501A variant of the 501 route) and then pick the variant that has the lowest average distance between
observed vehicle positions and the route variant
- to this list we add all of the directiontags that do accurately match on direction tag between the route and vehicle location
files.  This gives us a lookup table details the correct direction tag to use from the route list for each direction tag in the
vehicle location file

*/

drop table if exists calc_location_log_04;
with a as (
select pg.directiontag, pos.dirtag,
    avg(st_distance(pg.path_geom, pos.prev_geom_position)) avg_error,
    row_number() over (partition by pos.dirtag order by avg(st_distance(pg.path_geom, pos.prev_geom_position))) error_order
from calc_location_log_03 pos
inner join calc_direction_paths pg on pos.routetag = pg.routetag
    and substring(pg.directiontag,0,position('_' in pg.directiontag)+2) = substring(pos.dirtag,0,position('_' in pos.dirtag)+2)
where pos.dirtag not in (select distinct directiontag from calc_direction_paths where directiontag is not null)
group by pg.directiontag, pos.dirtag
)

select directiontag, dirtag
into calc_location_log_04
from a
where error_order = 1;

insert into calc_location_log_04
select distinct pg.directiontag, pos.dirtag
from calc_location_log_03 pos
inner join calc_direction_paths pg on pg.directiontag = pos.dirtag;

create index inx_calc_location_log_04_directiontag on calc_location_log_04 (directiontag);
create index inx_calc_location_log_04_dirtag on calc_location_log_04 (dirtag);

/*
calc_location_log_05:
- percent_to_next_point -- this is how far along in the time period between the two measurement times for each reading the
analysis time is.
- prev_path_pos/aft_path_pos -- this is the location of where on the route the previous/after points are.  0 is at the
the start of the route, 1 is at the end
- flag_offpath -- if the vehicle position is more than 20 meters off the path we will flag in this field so we can investigate
*/
drop materialized view if exists calc_location_log_05 cascade;
create materialized view calc_location_log_05 as 
select pos.id, pos.analysis_time, pos.prev_report_time, pos.aft_report_time, 
    (pos.analysis_time - pos.prev_report_time)::float/(pos.aft_report_time - pos.prev_report_time)::float percent_to_next_point,
    pg.directiontag dirtag, pos.routetag, pg.path_geom,
    st_linelocatepoint(pg.path_geom, pos.prev_geom_position) prev_path_pos, 
    st_linelocatepoint(pg.path_geom, pos.aft_geom_position) aft_path_pos,
    case when st_distance(pg.path_geom::geography, pos.prev_geom_position::geography) > 20 then 1 else 0 end flag_offpath
from calc_location_log_03 pos
inner join calc_location_log_04 dir on dir.dirtag = pos.dirtag
inner join calc_direction_paths pg
    on ((dir.directiontag = pg.directiontag and pg.directiontag <> pos.dirtag)
        or pg.directiontag=pos.dirtag);

/*
-- uncomment if running sections manually and you want to reset the removes table
delete from calc_location_removes;
refresh materialized view calc_location_log_03;
refresh materialized view calc_location_log_05;
*/
                
--we will flag records more than 20meter from the route to be removed.
--we can analyse those records later on; when there are concentrations of vehicles off-route on certain route/times
--that likely indicates a detour
insert into calc_location_removes (id, remove_reason, analysis_time)
select id, 'more than 20m off route', analysis_time
from calc_location_log_05 where flag_offpath = 1;

--we refresh the two matviews which would be affected by removed records at this point
refresh materialized view calc_location_log_03;
refresh materialized view calc_location_log_05;

--if the vehicle has only progressed in a certain direction for less than 5min we will remove those records
with r as (
select id, analysis_time, dirtag,
    row_number() over (partition by id order by analysis_time) 
    - row_number() over (partition by id, dirtag order by analysis_time) gid
from calc_location_log_05
),

a as (
select id, dirtag, gid
from r 
group by id, dirtag, gid
having (max(analysis_time) - min(analysis_time)) < 300
)
insert into calc_location_removes (id, remove_reason, analysis_time)
select a.id, 'direction duration < 5min', r.analysis_time
from r inner join a on a.id = r.id and a.dirtag = r.dirtag and a.gid = r.gid;
refresh materialized view calc_location_log_03;
refresh materialized view calc_location_log_05;

/*
calc_location_log_06:
- this calculates at the analysis_time, what the curent position is as well as what the position was
at the previous analysis time.
- the analysis_path is the route that goes from the posision at the previous analysis time to the postion at the
current analysis time
*/
drop materialized view if exists calc_location_log_06 cascade;
create materialized view calc_location_log_06 as 
with cp as (
select *, prev_path_pos + (aft_path_pos - prev_path_pos)*percent_to_next_point cur_path_pos
from calc_location_log_05 m where flag_offpath <> 1),

o as (
select *,
    lag(cur_path_pos) over (partition by id, dirtag order by analysis_time) lag_path_pos,
    lag(dirtag) over (partition by id order by analysis_time) lag_dirtag,
    lag(dirtag) over (partition by id order by analysis_time desc) next_dirtag,
    lag(analysis_time) over (partition by id order by analysis_time) lag_analysis_time
from cp
),

pth as (
select o.id, o.analysis_time, o.dirtag, o.routetag, o.path_geom,o.lag_dirtag, o.next_dirtag,o.lag_analysis_time,
    case when o.cur_path_pos < o.lag_path_pos then o.cur_path_pos else o.lag_path_pos end st_path_pos,
    case when o.cur_path_pos >= o.lag_path_pos then o.cur_path_pos else o.lag_path_pos end end_path_pos,
    case when o.cur_path_pos < o.lag_path_pos then 1 else 0 end as flag_reversed
from o
where o.lag_path_pos is not null
)

select *, st_linesubstring(path_geom, 
   case when lag_dirtag<>dirtag or lag_analysis_time < (analysis_time - 300)
   then end_path_pos else st_path_pos end, 
   end_path_pos) analysis_path
from pth;


--if the vehicle has completed less than 20% of the route path before changing direction we will remove this
-- data from the analysis
with r as (
select id, analysis_time, dirtag, st_path_pos, end_path_pos, 
    row_number() over (partition by id order by analysis_time) 
    - row_number() over (partition by id, dirtag order by analysis_time) gid
from calc_location_log_06
),

a as (
select id, dirtag, gid
from r 
group by id, dirtag, gid
having (max(end_path_pos) - min(st_path_pos)) < 0.2)

insert into calc_location_removes (id, remove_reason, analysis_time)
select a.id, 'completed < 20% of run', r.analysis_time
from r inner join a on a.id = r.id and a.dirtag = r.dirtag and a.gid = r.gid;
refresh materialized view calc_location_log_03;
refresh materialized view calc_location_log_05;
refresh materialized view calc_location_log_06;


/*
calc_location_log_07:
- we will update the start and end postions so that the first record in a direction starts at 0 ad ends at 1.
We do this because the start and end stops are located at 0 and 1 and we want to make sure we record the vehicle
being at that postion at the start of the run even if the first reading from that run is afte the starting postion
*/
drop table if exists calc_location_log_07;
select id, analysis_time, dirtag, routetag, path_geom, lag_dirtag, next_dirtag, lag_analysis_time,
    case when lag_dirtag <> dirtag and st_path_pos < 0.05 then 0 else st_path_pos end st_path_pos,
    case when next_dirtag <> dirtag and end_path_pos > 0.95 then 1 else end_path_pos end end_path_pos, 
    flag_reversed, analysis_path
into calc_location_log_07
from calc_location_log_06;

        
--##############################################
--################PART 4########################
--#############Analysis#########################
--##############################################

/*
calc_runs_01:
- here we group the vehicle location records from the location log into 'runs' ie groups of consecutive records
where the vehicle is traveling in the same direction (ie having the same 'dirtag')
*/
drop table if exists calc_runs_01;
        
with r as (
select id, analysis_time, 
    row_number() over (partition by id order by analysis_time) 
    - row_number() over (partition by id, dirtag order by analysis_time) gid
from calc_location_log_07
)
        
select l.id, r.gid, l.dirtag, l.routetag, min(l.analysis_time) min_analysis_time, max(l.analysis_time) max_analysis_time,
    (-min(l.analysis_time)+max(l.analysis_time))/60 run_time_minutes, min(l.st_path_pos) min_path_pos, 
    max(l.end_path_pos) max_path_pos, sum(l.flag_reversed) flag_reversed_count, count(*) record_count,
    case when min(l.st_path_pos) < 0.05 then 1 else 0 end flag_run_from_start, 
    case when max(l.end_path_pos) > 0.95 then 1 else 0 end flag_run_to_end,
    max(l.end_path_pos) - min(l.st_path_pos) percent_of_run_covered,
    case when x.ma - max(l.analysis_time) < 300 then 1 else 0 end eof_cutoff
into calc_runs_01
from calc_location_log_07 l
cross join (select max(analysis_time) ma from calc_location_log_06) x
inner join r on r.id = l.id and r.analysis_time = l.analysis_time
group by l.id, r.gid, l.dirtag, l.routetag,x.ma
order by l.id, min(l.analysis_time);



/*
calc_stops_01_locations:
- create table of stops with the locations of all of the stops

*/

drop table  if exists calc_stops_01_locations;
with d as (
select ds.oid, ds.tag, ds.directiontag, ds.routetag,
    st_setsrid(st_makepoint(rs.lon::double precision, rs.lat::double precision), 4326) AS stop_geom
from tbl_ttc_dir_stop ds
inner join tbl_ttc_route_stop rs on rs.tag = ds.tag and ds.routetag = rs.routetag
),

c as (
select d.oid, d.tag stop_tag, p.directiontag, p.routetag, d.stop_geom,
    st_linelocatepoint(p.path_geom, d.stop_geom) stop_pos
from d
inner join calc_direction_paths p
on p.directiontag = d.directiontag
),

cent as (
select stop_tag, st_centroid(st_union(stop_geom)) stop_geom
from c 
group by stop_tag

)


select c.oid, c.stop_tag, c.directiontag, c.routetag, cent.stop_geom, c.stop_pos
into calc_stops_01_locations
from c
inner join cent on cent.stop_tag = c.stop_tag;

                             
create index inx_calc_stops_01_locations_stop_tag on calc_stops_01_locations (stop_tag);
                             
/*
calc_stops_02_pickups:
- finds the analysis_times at which vehicles pass a stop.. the assumption here is that this is when passengers would get picked
up from a stop
*/
drop table  if exists calc_stops_02_pickups;
select s.oid, s.stop_tag, l.dirtag, l.routetag, s.stop_geom, l.analysis_time, l.id
into calc_stops_02_pickups
from calc_location_log_07 l
inner join calc_stops_01_locations s
    on s.stop_pos between l.st_path_pos and l.end_path_pos
    and s.directiontag=l.dirtag;



                             
/*
calc_waittimes:
- we want to see how long the wait time between vehicles are at each stop
*/
drop table  if exists calc_waittimes;
with a as (
select p.stop_tag, a.analysis_time, min(p.analysis_time) - a.analysis_time waittime, a.hr, a.dt

from calc_stops_02_pickups p
inner join analysis_times a on a.analysis_time <= p.analysis_time and (a.analysis_time+5400>p.analysis_time)
group by p.stop_tag, a.analysis_time, a.hr, a.dt)

select a.stop_tag, hr, a.dt, avg(waittime) avg_waittime
into temp tmp_b
from a
group by a.stop_tag, a.hr, a.dt;

with sg as (select distinct stop_geom, stop_tag from calc_stops_01_locations)

select b.*, sg.stop_geom
into calc_waittimes
from tmp_b b
inner join sg on sg.stop_tag = b.stop_tag;

drop table tmp_b;
                             
                             

drop table  if exists toronto_streets_01;
select *, st_linemerge(geom) mergegeom, st_startpoint(st_linemerge(geom))::text start_geom, st_endpoint(st_linemerge(geom))::text end_geom
into toronto_streets_01
from toronto_streets
where fcode_desc in ('Collector','Major Arterial','Minor Arterial','Busway');

drop table  if exists toronto_streets_02_junctions;
select distinct ST_Intersection(a.geom, b.geom)
into toronto_streets_02_junctions
from toronto_streets_01 a inner join toronto_streets_01 b 
on a.geo_id <> b.geo_id 
    and a.start_geom <> b.end_geom
    and a.end_geom <> b.start_geom

drop table  toronto_streets_03_main_intersections;
with sag as (
select (st_dump(st_voronoipolygons(st_collect(st_intersection)))).geom stop_area_geom
from toronto_streets_02_junctions
),

ms as (
select distinct stop_area_geom
from sag
inner join (select distinct stop_geom from calc_stops_01_locations) c
    on st_contains(sag.stop_area_geom, c.stop_geom)
),

rm as (
select j.st_intersection
from toronto_streets_02_junctions j
left join ms on st_contains(ms.stop_area_geom, j.st_intersection)
where ms.stop_area_geom is null
),

i as (
select st_intersection
from toronto_streets_02_junctions
--union all select st_setsrid(st_makepoint(lon::float, lat::float), 4326)
--from ttc_subway_stations
)

select *
into toronto_streets_03_main_intersections
from (
select (st_dump(st_voronoipolygons(st_collect(st_intersection),0.007))).geom stop_area_geom, 'grouped' grp_type
from i
where st_intersection not in (select st_intersection from rm)

union all select (st_dump(st_voronoipolygons(st_collect(st_intersection)))).geom stop_area_geom, 'not grouped' grp_type
from i
where st_intersection not in (select st_intersection from rm)) x
                          

/*
calc_stops_01_route_core:
- In this query we are trying to define the 'core' sections of a route, so the stops which all branches of the routes variants stop at. This matches up well as a proxy in identifying which are the 'most important' sections of a route in some cases like the 501 Queen St route, where stops between Roncesvalles and Greenwood are serviced by all routes and are what I would consider 'most important' sections.  However, this doesn't work in all cases.. eg. the 32 Eglinton West route, only the stops from Keele to the Eglinton West (@ Allen Rd) subway station are on all routes, the section between Eglinton West and Eglinton Stations are not covered by all routes, but I would expect that to be one of the 'core' sections.
- this isn't really needed for the main part of the anlysis that i'm looking to do, but I'm going to keep this in here as I might want to revisit at a later date
                               
                               
drop table if exists calc_stops_01_route_core;
with a as (
select substring(directiontag,0,position('_' in directiontag)+2) route_dir, stop_tag, stop_geom, routetag, count(*) variant_count
from calc_stops_01_locations
group by substring(directiontag,0,position('_' in directiontag)+2), stop_geom, stop_tag, routetag
),

mv as (
select route_dir, max(variant_count) max_variants
from a
group by route_dir
),

c as (
select a.routetag, a.route_dir, a.stop_tag, max_variants

from a
inner join mv on mv.route_dir = a.route_dir and mv.max_variants = a.variant_count
),


d as (
select *, row_number() over (partition by directiontag order by stop_pos) coreoid
from calc_stops_01_locations l
inner join c
    on c.stop_tag = l.stop_tag and c.routetag = l.routetag
)


select directiontag, route_dir, min(stop_pos) min_stop_pos, max(stop_pos) max_stop_pos
into calc_stops_01_route_core
from d
group by directiontag, route_dir


select l.*, substring(l.directiontag,0,position('_' in l.directiontag)+2) route_dir,
    case when c.directiontag is null then 'non-core' else 'core' end core
from calc_stops_01_locations l
left join calc_stops_01_route_core c 
    on c.directiontag = l.directiontag
    and l.stop_pos >= c.min_stop_pos
    and l.stop_pos <= c.max_stop_pos
where l.routetag = '504_1'
*/


