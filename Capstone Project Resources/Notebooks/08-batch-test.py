dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
env = dbutils.widgets.get("Environment")


def run_notebook(notebook_path, timeout_seconds=600, parameters=None):
    if parameters is None:
        parameters = {}
    dbutils.notebook.run(notebook_path, timeout_seconds, parameters)


def validate_stage(stage, iterations):
    for i in range(1, iterations + 1):
        stage.validate(i)


def produce_and_validate(stage, iterations):
    for i in range(1, iterations + 1):
        stage.produce(i)
        stage.validate(i)


SH = SetupHelper(env)
SH.cleanup()

run_notebook("./07-run", 600, {"Environment": env, "RunType": "once"})

HL = HistoryLoader(env)
SH.validate()
HL.validate()

PR = Producer()
produce_and_validate(PR, 2)
run_notebook("./07-run", 600, {"Environment": env, "RunType": "once"})

for notebook in ["./04-bronze", "./05-silver", "./06-gold"]:
    run_notebook(notebook)

BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

validate_stage(BZ, 2)
validate_stage(SL, 2)
validate_stage(GL, 2)

SH.cleanup()
