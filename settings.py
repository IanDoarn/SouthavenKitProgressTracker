ORACLE_CONNECTION_STRING = r"logistics/log78gist@10.201.207.188:1521/smsprd"
PORSTGRES_CONNECTION_STRING = "dbname=postgres user=doarni host=vsbslgprd01.zmr.zimmer.com password=ZimmerBiomet"
NEW_KIT_REGEX_PATTERN = r"^([\d\w-]*)(\s{3})(serials) ([\d,]*)$"
KITS_TO_TRACK_TABLE = 'doarni.new_kits_to_track'
KIT_PROGRESS_TABLE = 'doarni.new_kit_progress_tracker'