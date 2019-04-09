[ -f ./sds.db ] && rm ./sds.db

echo "CREATE TABLE Attribute(attr_id INTEGER, attr_name TEXT, type TEXT);" | sqlite3 sds.db

echo "CREATE TABLE file_attribute_mapping(filehash TEXT, attr_id INTEGER, value TEXT);" | sqlite3 sds.db

echo "CREATE TABLE Ftable(path TEXT, AID INTEGER, Avalue FLOAT);" | sqlite3 sds.db
echo "CREATE TABLE Ttable(path TEXT, AID INTEGER, Avalue TEXT);" | sqlite3 sds.db
echo "CREATE TABLE Itable(path TEXT, AID INTEGER, Avalue INTEGER);" | sqlite3 sds.db