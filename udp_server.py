import socketserver
import logging

logger = logging.getLogger(__name__)


# class UDPServer(socketserver.UDPServer):
#     def __init__(self, server_address, HandlerClass, mvn_data_assembler):
#         super().__init__(server_address, HandlerClass)
#         self.mvn_data_assembler = mvn_data_assembler

class DatagramRequestHandler(socketserver.DatagramRequestHandler):
    """Define self.rfile and self.wfile for datagram sockets."""

    def __init__(self, mvn_data_assembler):
        self.mvn_data_assembler = mvn_data_assembler

    def setup(self):
        from io import BytesIO
        self.packet, self.socket = self.request
        self.rfile = BytesIO(self.packet)
        self.wfile = BytesIO()

    def finish(self):
        self.socket.sendto(self.wfile.getvalue(), self.client_address)


class BaseServer(socketserver.BaseServer):
    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)


class UDPHandler(DatagramRequestHandler):
    def handle(self):
        """
        Overrides the handle() method
        :return:
        """

        logger.info("Received request from {}".format(self.client_address[0]))
        message = self.rfile.readline().strip()
        logger.info("Datagram received from client is:".format(message))
        logger.info(message)
