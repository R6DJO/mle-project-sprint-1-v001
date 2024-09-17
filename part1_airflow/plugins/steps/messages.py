from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='telegram_r6djo',
                        token='2049318547:AAFXhcWe2aOIbgLvaTaYQjkcXhf2JmSJYrU',
                        chat_id='-1001492037392'
                        )
    # hook = TelegramHook(telegram_conn_id='telegram_r6djo')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        # 'chat_id': '-1001492037392',
        'text': message
    })

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='telegram_r6djo',
                        token='2049318547:AAFXhcWe2aOIbgLvaTaYQjkcXhf2JmSJYrU',
                        chat_id='-1001492037392'
                        )
    # hook = TelegramHook(telegram_conn_id='telegram_r6djo')
    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
	
    
    message = (f'Исполнение DAG {dag} с id={run_id} прошло с ошибкой!\n'
               f'{task_instance_key_str}')
	
    hook.send_message({
        # 'chat_id': '-1001492037392',
        'text': message
    })
