from eventlet import wsgi, monkey_patch
from adistools.adisconfig import adisconfig
from adistools.log import Log
from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from json import loads, dumps

monkey_patch()

from flask import Flask, render_template, request
from flask_socketio import SocketIO

import functools

class socketio_dispatcher:
    name="moma-socketio_dispatcher"

    def __init__(self, application, socketio):
        self.application=application
        self.socketio=socketio

        self.config=adisconfig('/opt/adistools/configs/moma-socketio_dispatcher.yaml')
        self.log=Log(
            parent=self,
            rabbitmq_host=self.config.rabbitmq.host,
            rabbitmq_port=self.config.rabbitmq.port,
            rabbitmq_user=self.config.rabbitmq.user,
            rabbitmq_passwd=self.config.rabbitmq.password,
            debug=self.config.log.debug,
            )

        self.rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                host=self.config.rabbitmq.host,
                port=self.config.rabbitmq.port,
                credentials=PlainCredentials(
                    self.config.rabbitmq.user,
                    self.config.rabbitmq.password
                )
            )
        )
        self.rabbitmq_channel = self.rabbitmq_conn.channel()

        self.rabbitmq_channel.basic_consume(
            queue='moma-events',
            auto_ack=True,
            on_message_callback=self.response_process
        )

        self.application.config['SECRET_KEY'] = self.config.socketio.secret

    def response_process(self, channel, method, properties, body):
        data=loads(body.decode('utf8'))
        self.socketio.emit(
            "event",
            data,
        )

    def start(self):
        try:
            self.socketio.start_background_task(target=self.rabbitmq_channel.start_consuming)
            self.socketio.run(self.application, host=self.config.socketio.host, port=self.config.socketio.port)
        except:
            self.stop()

    def stop(self):
        self.rabbitmq_channel.stop_consuming()

if __name__=="__main__":
    app = Flask(__name__)
    socketio = SocketIO(
        app,
        cors_allowed_origins="*")

    socketio_dispatcher=socketio_dispatcher(app, socketio)
    socketio_dispatcher.start()
