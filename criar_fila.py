import boto3

def criar_fila():
    sqs = boto3.client(
        "sqs",
        endpoint_url="http://localhost:9324",
        region_name="us-east-1",
        aws_access_key_id="x",
        aws_secret_access_key="x"
    )

    nome_fila = "guias"
    print(f"Criando fila '{nome_fila}' com VisibilityTimeout=15s...")
    
    try:
        resposta = sqs.create_queue(
            QueueName=nome_fila,
            Attributes={
                "VisibilityTimeout": "15"
            }
        )
        queue_url = resposta["QueueUrl"]
        print(f"Fila criada com sucesso: {queue_url}")
        
        # Validar atributos
        attrs = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["VisibilityTimeout", "QueueArn"]
        )
        print("Atributos da fila:", attrs.get("Attributes"))
    except Exception as e:
        print(f"Erro ao criar fila: {e}")

if __name__ == "__main__":
    criar_fila()
