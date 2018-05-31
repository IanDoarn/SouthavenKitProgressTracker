from enum import Enum
# ========
# General Settings
# ========
DEFAULT_VERBOSITY_LEVEL = 5
MAX_VERBOSITY_LEVEL = 5
# ========
# DB info
# ========
ORACLE_CONNECTION_STRING = r"logistics/log78gist@10.201.207.188:1521/smsprd"
PORSTGRES_CONNECTION_STRING = "dbname=postgres user=doarni host=vsbslgprd01.zmr.zimmer.com password=ZimmerBiomet"

KITS_TO_TRACK_TABLE = 'doarni.new_kits_to_track'
KIT_PROGRESS_TABLE = 'doarni.new_kit_progress_tracker'

# ========
# Regex and lambdas
# ========
NEW_KIT_REGEX_PATTERN = r"^([\d\w-]*)(\s{3})(serials) ([\d,]*)$"
TRUNCATE_TABLE = lambda x: r"TRUNCATE TABLE {} CONTINUE IDENTITY RESTRICT;".format(x)

# ========
# Verbosity enumeration table
# ========
class Verbosity(Enum):
    GENERAL = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    OTHER = 5