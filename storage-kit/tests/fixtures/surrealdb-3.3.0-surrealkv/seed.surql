DEFINE NAMESPACE vendor_migration;
USE NS vendor_migration;
DEFINE DATABASE surreal_330;
USE DB surreal_330;
DEFINE TABLE records SCHEMAFULL;
DEFINE FIELD label ON records TYPE string;
DEFINE FIELD sequence ON records TYPE int;
CREATE records:a SET label = 'alpha', sequence = 1;
CREATE records:b SET label = 'beta', sequence = 2;
