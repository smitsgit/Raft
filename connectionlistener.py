from interface import Interface


class ConnectionListner(Interface):
    def connected(self, name, client):
        pass

    def disconnected(self, name, client):
        pass
