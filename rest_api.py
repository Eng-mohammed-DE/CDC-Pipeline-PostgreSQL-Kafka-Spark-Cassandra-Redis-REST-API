from flask import Flask, request

app = Flask(__name__)

@app.route('/api', methods=['POST'])
def receive():
    print(request.json)
    return '', 200

app.run(port=5000)
