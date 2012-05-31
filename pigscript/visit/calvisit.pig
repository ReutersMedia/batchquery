pageViewLog = load '$input' using PigStorage('\t') AS ( pageview_id, profile_id, site_user_id, global_user_id, page_id, host_id, referrer_path, ReferrerHostID, ReferrerHostTypeID, referrer_query, section_id, tstamp:chararray, tsbin_15s, tsbin_30s, tsbin_1m, tsbin_2m, tsbin_15m, tsbin_1h, tsbin_5s, tsbin_1d);

viewingLog = load '$inputtmp' using PigStorage('\t') AS ( pageview_id, profile_id, site_user_id, global_user_id, page_id, host_id, referrer_path, ReferrerHostID, ReferrerHostTypeID, referrer_query, section_id, tstamp:chararray, tsbin_15s, tsbin_30s, tsbin_1m, tsbin_2m, tsbin_15m, tsbin_1h, tsbin_5s, tsbin_1d);

allVisitLog = UNION pageViewLog, viewingLog;

visitLog = DISTINCT allVisitLog;

visitList = GROUP visitLog BY site_user_id;

visitList2 = FOREACH visitList {
            latestTime = MAX(visitLog.tstamp);
            GENERATE group, latestTime as latestTime, visitLog;
}

SPLIT visitList2 INTO finishVisit IF latestTime < '$deadline', viewingLog2 OTHERWISE;

STORE finishVisit INTO '$result' USING PigStorage('\t');

viewingLog3 = FOREACH viewingLog2 {
              GENERATE FLATTEN(visitLog);
}

STORE viewingLog3 INTO '$tmpfolder' USING PigStorage('\t');
