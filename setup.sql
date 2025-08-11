-- On main branch
create table `annex-keys` (url varchar(1000) primary key, `annex-key` varchar(1000), key (`annex-key`, url));
create table `sources` (`annex-key` varchar(1000) primary key, `sources` json, `numSources` int generated always as (JSON_LENGTH(sources)) STORED, index (`numSources`));
create table `hashes` (`hash` varbinary(256), `hashType` enum('md5'), `annex-key` varchar(1000), primary key (`hash`, `hashType`), key (`annex-key`, `hashType`));

-- On personal branch

create table `local_keys` (`annex-key` varchar(1000) primary key);
create table `local_submissions` (`source` enum('archiveofourown.org','furaffinity.net','e621.net','gelbooru.com','rule34.us','danbooru.donmai.us','e6ai.net') NOT NULL, `id` int NOT NULL, `updated` date NOT NULL, `part` int NOT NULL, PRIMARY KEY (`source`,`id`,`updated`,`part`));