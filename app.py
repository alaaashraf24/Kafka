from flask import Flask, request, render_template_string
from flask_socketio import SocketIO, emit

app = Flask(__name__)
socketio = SocketIO(app)

@app.route('/')
def index():
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>WebSocket Refresh Example</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.4.0/socket.io.min.js"></script>
    </head>
    <body>
        <h1>WebSocket Refresh Example</h1>
        <p>The page will refresh when a POST request is received at /update.</p>

        <script>
            var socket = io();

            socket.on('connect', function() {
                console.log('Connected to WebSocket');
            });

            socket.on('refresh', function() {
                location.reload();
            });
        </script>
    </body>
    </html>
    """)

@app.route('/update', methods=['POST'])
def update():
    socketio.emit('refresh', {})
    return "Update Triggered", 200

@socketio.on('connect')
def handle_connect():
    print('Client connected')

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
