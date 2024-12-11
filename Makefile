default: test

data:
	mkdir -p data

data/initdb.sql:
	echo create table results \(\\n\
		hash text primary key,\\n\
		git_hash text,\\n\
		-- Config fields\\n\
		manidim int,\\n\
		manistyle text,\\n\
		init_idcs text,\\n\
		greedy boolean DEFAULT false,\\n\
		-- Results fields\\n\
		train_err double precision,\\n\
		test_err double precision,\\n\
		val_err double precision,\\n\
		-- Status fields\\n\
		scheduled boolean DEFAULT false,\\n\
		running boolean DEFAULT false,\\n\
		failed boolean DEFAULT false,\\n\
		completed boolean DEFAULT false,\\n\
		\"opti.test\" integer\\n\
	\) > data/initdb.sql


data/dbfields.yaml:
	echo hash > data/dbfields.yaml
	echo manidim >> data/dbfields.yaml
	echo "# comment" >> data/dbfields.yaml
	echo manistyle >> data/dbfields.yaml
	echo "" >> data/dbfields.yaml
	echo greedy >> data/dbfields.yaml
	echo init_idcs >> data/dbfields.yaml
	echo opti.test >> data/dbfields.yaml


.venv:
	python3 -m venv .venv
	.venv/bin/pip install -e .

data/test.db: data/initdb.sql | data
	sqlite3 data/test.db < data/initdb.sql

test: data/dbfields.yaml .venv data/test.db
	.venv/bin/python exp/__init__.py
	rm -f data/dbfields.yaml
	rm -f data/initdb.sql
	rm -f data/test.db

clean:
	rm -f data/dbfields.yaml
	rm -f data/initdb.sql
	rm -f data/test.db
