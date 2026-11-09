.PHONY: up down logs test venv notebook

up:            ## Démarre toute la stack
	docker compose up -d --build

down:          ## Arrête la stack (données conservées)
	docker compose down

logs:          ## Suit les logs du pipeline
	docker compose logs -f producer processor

venv:          ## Environnement Python local (tests + analyse)
	uv venv && uv pip install -e ".[analysis,dev]"
	.venv/bin/python -m ipykernel install --sys-prefix --name vigie --display-name "Python (vigie)"

test:          ## Tests unitaires
	.venv/bin/python -m pytest -q

notebook:      ## Ré-exécute le notebook d'analyse sur les données collectées
	cd notebooks && ../.venv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=vigie analyse_trafic.ipynb
