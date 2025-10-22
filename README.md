# nereliacines
### Aplinkos setup
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
./env/Scripts/acitvate
pip install -r requirements.txt
python front/manage.py migrate
```
### Projekto paleidimas
Paleisti projekta is projekto root aplanko ```/nereliacines```.

```
python start.py
```

Nepamirskit aktyvuoti savo virtualia aplinka.


Jeigu naudojat kazkokius naujus packages padarykit ```pip freeze > requirements.txt``` pries pushinant.