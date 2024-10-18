from airflow.providers.telegram.hooks.telegram import TelegramHook


def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id="telegram_r6djo")
    dag = context["dag"].dag_id
    run_id = context["run_id"]

    message = f"Успешно!\nDAG {dag}\nid={run_id}"

    hook.send_message({"text": message})


def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id="telegram_r6djo")
    dag = context["dag"].dag_id
    run_id = context["run_id"]
    task_instance_key_str = context["task_instance_key_str"]

    message = (
        f"Ошибка!!!\nDAG {dag}\nid={run_id}"
        f"{task_instance_key_str}"
    )

    hook.send_message({"text": message})
