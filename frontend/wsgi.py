from project.server import create_app, socketio
#from flask_socketio import SocketIO

app = create_app()
app.debug = True

if __name__ == "__main__":
  socketio.run(app, host='0.0.0.0', port=5011, use_reloader=True, debug=True)

