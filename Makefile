initdb.sql:
	echo create table results\(\\n\
		hash text primary key,\\n\
		-- Config fields\\n\
		manidim int NOT NULL,\\n\
		manistyle text NOT NULL,\\n\
		greedy boolean DEFAULT false,\\n\
		-- Results fields\\n\
		train_err double precision,\\n\
		test_err double precision,\\n\
		val_err double precision,\\n\
		-- Status fields\\n\
		scheduled boolean DEFAULT false,\\n\
		running boolean DEFAULT false,\\n\
		failed boolean DEFAULT false,\\n\
		completed boolean DEFAULT false\\n\
	\) >> initdb.sql

tt:
	echo a\
		bv  >> test.txt

dbfields.yaml:
	echo - [hash, manidim, manistyle, greedy] > dbfields.yaml
	echo - [train_err, test_err] >> dbfields.yaml


.venv:
	python3 -m venv .venv
	.venv/bin/pip install -e .

test: dbfields.yaml initdb.sql .venv
	rm data/test.db
	mkdir -p data
	sqlite3 data/test.db < initdb.sql
	.venv/bin/python exp/__init__.py
