# --- !Ups

--- This migration will break most of the existing data sources.

--adding hana
INSERT INTO data_source_products VALUES(true, 'hana', 'com.sap.db.jdbc.Driver',  uuid_generate_v4(), 'SAP Hana', null, 'hana');

UPDATE data_source_products SET class_name='org.mariadb.jdbc.Driver' where product_root='mysql';
UPDATE data_source_products SET class_name='org.postgresql.Driver', is_bundled=true, product_root='postgresql' where name='PostgreSQL';

UPDATE data_source_products SET type=product_root where is_bundled = true AND product_root NOT IN ('', 'bigquery');

INSERT
INTO data_source_product_icons
VALUES(
(SELECT data_source_product_id FROM data_source_products WHERE is_bundled = true and product_root='hana'),
 bytea E'\\xFFD8FFE000104A46494600010100004800480000FFE1008C4578696600004D4D002A000000080005011200030000000100010000011A0005000000010000004A011B0005000000010000005201280003000000010002000087690004000000010000005A00000000000000480000000100000048000000010003A00100030000000100010000A00200040000000100000060A0030004000000010000003600000000FFED003850686F746F73686F7020332E30003842494D04040000000000003842494D0425000000000010D41D8CD98F00B204E9800998ECF8427EFFC20011080036006003012200021101031101FFC4001F0000010501010101010100000000000000030204010500060708090A0BFFC400C31000010303020403040604070604080673010200031104122105311322100641513214617123078120914215A15233B124623016C172D14392348208E1534025631735F09373A25044B283F1265436649474C260D284A31870E227453765B35575A495C385F2D3467680E3475666B4090A191A28292A38393A48494A5758595A6768696A7778797A868788898A90969798999AA0A5A6A7A8A9AAB0B5B6B7B8B9BAC0C4C5C6C7C8C9CAD0D4D5D6D7D8D9DAE0E4E5E6E7E8E9EAF3F4F5F6F7F8F9FAFFC4001F0100030101010101010101010000000000010200030405060708090A0BFFC400C31100020201030303020305020502040487010002110310122104203141130530223251144006332361421571523481502491A143B11607623553F0D12560C144E172F117826336702645549227A2D208090A18191A28292A3738393A464748494A55565758595A6465666768696A737475767778797A80838485868788898A90939495969798999AA0A3A4A5A6A7A8A9AAB0B2B3B4B5B6B7B8B9BAC0C2C3C4C5C6C7C8C9CAD0D3D4D5D6D7D8D9DAE0E2E3E4E5E6E7E8E9EAF2F3F4F5F6F7F8F9FAFFDB00430006040506050406060506070706080A100A0A09090A140E0F0C1017141818171416161A1D251F1A1B231C1616202C20232627292A29191F2D302D283025282928FFDB0043010707070A080A130A0A13281A161A2828282828282828282828282828282828282828282828282828282828282828282828282828282828282828282828282828FFDA000C03010002110311000001D2F9FF00BBF2B458576C953A6D6AA66F6881D62D3D0F0EA2936F23E869AAD8DD76F1F15E8B47625286F12A56AD961D632ADFEDC5DDB6C1BCD5EF1FBDAF22FEA1BED13A64F39DEF3EEF1E5A32E0EB68B95B6437CC5E2E9E3731BD9E19D1ABAFF41E57AAF1FD142F6E7D76DA3B6D5FFFDA0008010100010502FBF57576702EE248D02343B4B396E5C9B5CA94F036FB7C93C32A0C52DB58493C276A9A92A1512EDA155C496F0A608FB5A0E66D823BFB56B52E79F988B756F71E373B67FB4CDBBDF7DE3778CCB75676C9B68BBA2D6F6D21DB2EE7B82214AF7DB8F7355CEF28E6D8ED069B62777B991D9DB9887DCB3DD25B6449BDC85365B82AD8AD656B8B745A2D6D3725C16DB4D87BB226BBE54F25E2936B3DE72CFBD9F77B6979D0FDDD9AC39612A0A6BB60B9BDC50422CC009B1488EDA0100FB9B1592662C240FBDFFFDA0008010311013F018F479651DC038FA79E43510C3A69CE5B62397274B9317320F45D19CA774BC20570C7EEA8CAC1706338F74BD6D863DBD413F986118E587B62C863111143419B20140A6448A2E239272E0B2818C00C4D751FED1C7EF6E1BFC6B84010ECFFDA0008010211013F0139F1C4ED259E48C05C99648C46E2C32C27F84BD4E7F6C50F3A486DB94688734C64A1FD194F7601FD0B332C52F724CA4646CE8706326C84400361CD1C58E3643BC4A44E46F0B3F6EBEDD7AA91390F67FFDA0008010100063F02FE66809091C4B094F01DAA9A04FA9754A92BF870EC240A4807D5A90AE20D18912A4807D5E8A41652B14218423F1F46108EE84C4AC49452BE85A8A6930F9D59AFB6A2EDEDFF006B41F634AFC9618A71EA69E67330FCD9B8638C56421E2353E67D7EE7361583FC84EAD4268A800F6A946A29E08199F9B4AE59C0923FE5D28F34EB875349FED7F0B094471E47868CAE5394EBF695FD5F770A05A070AF93A4712507D6B5721C02D4B352A516A51E24D5880C6950C71A92C409892AFB5F3251F4C47F82D485620045457CCB8A408056BD68E134AA162A4FA35C81209CF148F5695F03E63EF09E71D7F941F27A35C84FB48C5A02D454109C4071852B208053C38D5A50544A02B26A0924A49AFCBEEF3E4D424D027E3FCC7FFFC40033100100030002020202020301010000020B0111002131415161718191A1B1C1F0D110E1F12030405060708090A0B0C0D0E0FFDA0008010100013F219B366CD9B367FE27E5BCEA078FFF0068470507FC2189B1FEBAF141F2565071321BC52B494D8A2DCB2A98C94E34925BC6956BF3869D1EDE83CD0A30E5795F3FF78BBC1FB3F74B6A4E170F70D50DC7E2378B06747EA58A4FEC1FF966EFC423E5BEAFFA38B33A193D4F749EEEF9DFFE0641185E59E7FF009763D51427C6D2008B0FAC5E29383A9BC51EC80A78EFF5602EBFB2A4F2300997F37FC23C87AFFF000957C30E1F41AD913B67F5175DBC19FE2E80A97DB441FD0CA7E28A8490552CBE23DD890628F0F1F34F7C097D14D1272F006D128F96833FDDCA0511F2CB361267812CD9B366CDC555BD6F97DFF1653C2BC19C31C7BBE02AA3EEB61819E0A59FA8713F7637A10FFF0084961F6E876FFC673FFC5FFFDA000C03010002110311000010A4E66FDBFF000BDDFF00FC7CD4956E88821FFF003CFFC4003311010101000300010205050101000101090100112131104151612071F09181A1B1D1C1E1F130405060708090A0B0C0D0E0FFDA0008010311013F10D574FD7F322DC924D83B20B505F1BFFD7EDFE6000E8B1983304EBF5F9973A1AC6BF20F3FBFF784EE91FEA7FB9E3E8D57FB1023C0F3A948B370438E83373A38FF00170478DA29B9A63AE1337F7E7FE4747C18F277DEFF00AB2C2318770075EFFFDA0008010211013F10C89DB40610F5C191CD21F9B7F4955D642C5BCEF7319B86B0F8538FDA61BDE1FD1FF519007300F9FBB206D5F3BD5FD7F1362E590769DCD797F4C7CDBB0CE37BFAFCEE7EDC5CF3B6F1F97D3FDFA04BD752EF7EFF00FFDA0008010100013F10FF00F1FF009797F37FFA0D270880B9F07B75F9B9DBA267F2F6FF00C166589A17B01AC7E29F7A980A7C4E2D606A941089C89D367104F14189CF317754A0E1E34F488D76D60C090EBE2BEE8494DF71597147E823D8F9BB72751EC2FE8EDAEF8DFE524F97FEE208972C70A3CEAF39010178693EA76C550CE376833AD8A4041F58397ED8FCDC721CA1EB77E47E2B6FEC04E7146119BD2E113B33149DC5CEA085741ACD48486B21FE80E03AFF00F033D89616F14860F098ABD0809D28C726372C2C02F0141F6CCFD5903F21E45865F99AB186AC91E30F24A6A4516627862ADE1D4A43A1FF00E2C46F0A70471E11FBE7FF00C242DA1D378239E933CD2E050397B101F968149841C1C629D57AE6AC7F25D45FE6B1B93060468238F762BC1AD99882D9804ED99A03B31E1EDDBF5C533A40C1598FBC5697373428AF399F9B097485F247C72F8B2A0D250F1F91B6356481E7227E4FFF000870760355E0B39C082D438BA67074F6B5A099E52598884E53D53DD1EC27132BAD367A8E2C0A3A60858FC455B83DE9A402348F2544A23FB743DF1DD9B366CDD0EF224D5F11D1E75EA1044746BA67291AFF00F8BFFFD9'
);

--removing product properties for bundled products
DELETE
FROM data_source_product_properties
WHERE data_source_product_id IN (SELECT data_source_product_id from data_source_products where is_bundled=true);


--setting up product properties for bundled products
WITH bundledProducts AS (SELECT data_source_product_id as id, product_root as root from data_source_products where is_bundled=true)
INSERT
INTO data_source_product_properties
VALUES
-----------------------------------------------------------------------postgresql
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='postgresql'),
  'host',
  'text',
  1
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='postgresql'),
  'port',
  'text',
  2
),
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='postgresql'),
  'database',
  'text',
  3
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='postgresql'),
  'schema',
  'text',
  4
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='postgresql'),
  'user',
  'text',
  5
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='postgresql'),
  'password',
  'password',
  6
)
,
-----------------------------------------------------------------------clickhouse
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='clickhouse'),
  'host',
  'text',
  1
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='clickhouse'),
  'port',
  'text',
  2
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='clickhouse'),
  'database',
  'text',
  3
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='clickhouse'),
  'user',
  'text',
  4
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='clickhouse'),
  'password',
  'password',
  5
),
-----------------------------------------------------------------------mysql
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='mysql'),
  'host',
  'text',
  1
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='mysql'),
  'port',
  'text',
  2
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='mysql'),
  'database',
  'text',
  3
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='mysql'),
  'protocol',
  'text',
  4
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='mysql'),
  'user',
  'text',
  5
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='mysql'),
  'password',
  'password',
  6
),
-----------------------------------------------------------------------hana
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='hana'),
  'host',
  'text',
  1
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='hana'),
  'port',
  'text',
  2
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='hana'),
  'database',
  'text',
  3
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='hana'),
  'schema',
  'text',
  4
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='hana'),
  'user',
  'text',
  5
)
,
(
  uuid_generate_v4(),
  (SELECT id FROM bundledProducts WHERE root='hana'),
  'password',
  'password',
  6
);

# --- !Downs