create table `annex-keys` (url varchar(1000) primary key, `annex-key` varchar(1000), key (`annex-key`, url));
create table `sources` (`annex-key` varchar(1000) primary key, `sources` json, `numSources` int generated stored always as (JSON_LENGTH(sources)) STORED, index (`numSources`));
