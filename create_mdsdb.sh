[ -f ./mmu.db ] && rm ./mmu.db

echo "CREATE TABLE template_nspace(tnid INTEGER, Name TEXT, tscope TEXT);" | sqlite3 mmu.db

echo "CREATE TABLE collaboration_nspace(dcenter TEXT, nofdtns INTEGER, dcip TEXT, dtnpath TEXT, tnid INTEGER);" | sqlite3 mmu.db

echo "CREATE TABLE namespace(dcenter TEXT,nofdtns INTEGER, dcip TEXT, dtnpath TEXT, tnid INTEGER);" | sqlite3 mmu.db

echo "CREATE TABLE file_path_mapping(filehash TEXT, file_name TEXT, file_path TEXT, tnid INTEGER);" | sqlite3 mmu.db
