NS_SERVER_TIMEOUT = 120
COUCHBASE_SINGLE_DEFAULT_INI_PATH = "/opt/couchbase/etc/couchdb/default.ini"
MEMBASE_DATA_PATH = "/opt/membase/var/lib/membase/data/"
MEMBASE_VERSIONS = ["1.5.4", "1.6.5.4-win64", "1.7.0", "1.7.1", "1.7.1.1", "1.7.2"]
COUCHBASE_DATA_PATH = "/opt/couchbase/var/lib/couchbase/data/"
# remember update WIN_REGISTER_ID also when update COUCHBASE_VERSION
COUCHBASE_VERSIONS = ["1.8.0r", "1.8.0", "1.8.1", "2.0.0", "2.0.1", "2.0.2", "2.1.0", "2.1.1", "2.2.0",
                      "2.2.1", "2.5.0"]
WIN_MEMBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Membase/Server/var/lib/membase/data/'
WIN_COUCHBASE_DATA_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/data/'
WIN_CB_PATH = "/cygdrive/c/Program Files/Couchbase/Server/"
WIN_MB_PATH = "/cygdrive/c/Program Files/Membase/Server/"
LINUX_CB_PATH = "/opt/couchbase/"
WIN_REGISTER_ID = {"1654":"70668C6B-E469-4B72-8FAD-9420736AAF8F", "170":"AF3F80E5-2CA3-409C-B59B-6E0DC805BC3F", \
                   "171":"73C5B189-9720-4719-8577-04B72C9DC5A2", "1711":"73C5B189-9720-4719-8577-04B72C9DC5A2", \
                   "172":"374CF2EC-1FBE-4BF1-880B-B58A86522BC8", "180":"D21F6541-E7EA-4B0D-B20B-4DDBAF56882B", \
                   "181":"A68267DB-875D-43FA-B8AB-423039843F02", "200":"9E3DC4AA-46D9-4B30-9643-2A97169F02A7", \
                   "201":"4D3F9646-294F-4167-8240-768C5CE2157A", "202":"7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "210":"7EDC64EF-43AD-48BA-ADB3-3863627881B8", "211":"7EDC64EF-43AD-48BA-ADB3-3863627881B8",
                   "220":"CC4CF619-03B8-462A-8CCE-7CA1C22B337B", "221":"3A60B9BB-977B-0424-2955-75346C04C586",
		   "250":"2B630EB8-BBC7-6FE4-C9B8-D8843EB1EFFA"}
VERSION_FILE = "VERSION.txt"
MIN_COMPACTION_THRESHOLD = 2
MAX_COMPACTION_THRESHOLD = 100
NUM_ERLANG_THREADS = 16
LINUX_COUCHBASE_BIN_PATH = "/opt/couchbase/bin/"
WIN_COUCHBASE_BIN_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/bin/'
WIN_COUCHBASE_BIN_PATH_RAW = 'C:\Program Files\Couchbase\Server\\bin\\'
WIN_TMP_PATH = '/cygdrive/c/tmp/'
MAC_COUCHBASE_BIN_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/bin/"
MAC_CB_PATH = "/Applications/Couchbase\ Server.app/Contents/Resources/couchbase-core/"
LINUX_COUCHBASE_LOGS_PATH = '/opt/couchbase/var/lib/couchbase/logs'
WIN_COUCHBASE_LOGS_PATH = '/cygdrive/c/Program\ Files/Couchbase/Server/var/lib/couchbase/logs/'
MISSING_UBUNTU_LIB = ["libcurl3"]
LINUX_GOPATH = '/root/tuq/gocode'
WINDOWS_GOPATH = '/cygdrive/c/tuq/gocode'
LINUX_GOROOT = '/root/tuq/go'
WINDOWS_GOROOT = '/cygdrive/c/Go'
