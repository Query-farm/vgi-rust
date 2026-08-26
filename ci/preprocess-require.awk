# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Rewrite each `require <ext>` gate in an upstream vgi sqllogictest into an
# explicit LOAD/INSTALL+LOAD statements, so the prebuilt standalone
# `haybarn-unittest` (which links none of these extensions) can run the suite.
# VGI loads from the exact source-built artifact supplied with `-v
# vgi_extension=...`; httpfs/json/parquet/spatial come from the signed core
# channel. `require-env` and every other directive pass through untouched. See
# ci/README.md.
#
# With `-v http=1`, also inject a signed `INSTALL httpfs FROM core; LOAD httpfs;`
# before the first worker ATTACH (keyed off `require vgi` or `require-env
# VGI_TEST_WORKER`). The prebuilt `haybarn-unittest` does not statically link
# httpfs, so `ATTACH ... (TYPE vgi, LOCATION 'http://...')` fails with a binder
# error unless httpfs is loaded into the connection first.
BEGIN { injected = 0 }
function sql_quote(value, quoted) {
    quoted = value
    gsub(/'/, "''", quoted)
    return "'" quoted "'"
}
function inject_httpfs() {
    if (http != 1 || injected) return
    print "";
    print "statement ok"; print "INSTALL httpfs FROM core;"; print "";
    print "statement ok"; print "LOAD httpfs;";
    injected = 1
}
/^require[ \t]+vgi[ \t]*$/ {
    if (vgi_extension == "") {
        print "preprocess-require.awk: missing -v vgi_extension=..." > "/dev/stderr"
        exit 2
    }
    print "statement ok"; print "LOAD " sql_quote(vgi_extension) ";";
    inject_httpfs();
    next
}
/^require[ \t]+(httpfs|json|parquet|spatial)[ \t]*$/ {
    ext = $2
    print "statement ok"; print "INSTALL " ext " FROM core;"; print "";
    print "statement ok"; print "LOAD " ext ";"; next
}
/^require-env[ \t]+VGI_TEST_WORKER[ \t]*$/ {
    print
    inject_httpfs();
    next
}
{ print }
