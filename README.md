# Prova 2 - Módulo 9

## Como instalar

Após clonar o repositório, navegue até a pasta do projeto e instale as dependências necessárias:

```bash
pip install -r requirements.txt
```

## Como rodar o sistema
Para iniciar o script que produz e consome mensagens, execute o seguinte comando:

```bash
python3 main.py
```

## Executar os Testes

Para executar os testes automatizados, use o seguinte comando:

```bash
pytest
```

## Demonstração do Sistema
https://youtu.be/DyNiBPCHwvA


## Considerações
Para comprovar que o código desenvolvido permite a modificação do formato de mensagem sem precisar de um refatoramento substancial, basta alterar a função presente no arquivo 'main':
```python
def generate_sensor_message():
    payload = {
        "idSensor": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "tipoPoluente": sensor_type[random.randint(0, 6)],
        "nivel": round(random.uniform(0, 100), 1),
    }
    return payload
```

