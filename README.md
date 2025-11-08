# nereliacines
### Aplinkos setup
Kad veiktu kasandra reikia atsisiuti `docker` ir `docker-compose`


macOS
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
python front/manage.py migrate
```

Windows (nezinau ar teisingai)
```
python3 -m venv env
./env/Scripts/activate
pip install -r requirements.txt
python front/manage.py migrate
```

### Projekto paleidimas
Toliau aprasytas komandas leisti is projekto root aplanko `/nereliacines`
Paleisti **kasandros** 👁️ duomenų baze naudojant
```
docker-compose up -d
```

Tada pirmam kartui paleidziame sita, kad sukurtume lenteles (turetu reiketi sukurti tik pirma karta pasijungus, kai lenteliu dar nera)
```
python init-scripts/init_cassandra.py
```

Paleisti front ir back dali
```
python start.py
```

Nepamirskit aktyvuoti savo virtualia aplinka.


Jeigu naudojat kazkokius naujus packages padarykit ```pip freeze > requirements.txt``` pries pushinant.
