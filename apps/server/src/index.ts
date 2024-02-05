import http from 'http'
import SocketService from './services/socket';
import {startMessageConsumer} from './services/kafka'
import { start } from 'repl';

async function init() {
    startMessageConsumer();
    const socketService = new SocketService();

    const httpServer = http.createServer();
    const PORT = process.env.PORT || 8000;

    socketService.io.attach(httpServer);

    httpServer.listen(PORT, () => {
        console.log(`Server listening on port ${PORT}`)
    }); 

    socketService.initListeners();
}

init(); 