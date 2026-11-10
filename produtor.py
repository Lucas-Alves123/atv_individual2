import json, boto3

sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:9324",
    region_name="us-east-1",
    aws_access_key_id="x",
    aws_secret_access_key="x"
)
url = sqs.get_queue_url(QueueName="guias")["QueueUrl"]

print("Iniciando publicação de mensagens...")

for i in range(1, 21):
    pedido = {"requisicaoId": f"R-{i:03d}", "hospital": "H-Recife"}
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps(pedido))

# Mensagem inválida (sem requisicaoId)
sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"hospital": "H-Recife"}))

print("21 mensagens enviadas com sucesso (20 válidas e 1 inválida).")

