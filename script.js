class ChessGame {
    constructor() {
        this.board = [];
        this.currentPlayer = 'white';
        this.selectedSquare = null;
        this.gameHistory = [];
        this.capturedPieces = { white: [], black: [] };
        this.isGameOver = false;
        this.isAIThinking = false;
        
        this.pieces = {
            white: {
                king: '💻', queen: '📱', rook: '▣', 
                bishop: '🍪', knight: '🦠', pawn: '👨‍💻'
            },
            black: {
                king: '💻', queen: '📱', rook: '▣', 
                bishop: '🍪', knight: '🦠', pawn: '👨‍💻'
            }
        };
        
        this.initializeBoard();
        this.renderBoard();
        this.setupEventListeners();
    }
    
    initializeBoard() {
        // Inicializar tablero vacío
        this.board = Array(8).fill().map(() => Array(8).fill(null));
        
        // Configurar piezas iniciales
        const initialSetup = [
            ['rook', 'knight', 'bishop', 'queen', 'king', 'bishop', 'knight', 'rook'],
            ['pawn', 'pawn', 'pawn', 'pawn', 'pawn', 'pawn', 'pawn', 'pawn']
        ];
        
        // Piezas negras
        for (let col = 0; col < 8; col++) {
            this.board[0][col] = { type: initialSetup[0][col], color: 'black' };
            this.board[1][col] = { type: initialSetup[1][col], color: 'black' };
        }
        
        // Piezas blancas
        for (let col = 0; col < 8; col++) {
            this.board[7][col] = { type: initialSetup[0][col], color: 'white' };
            this.board[6][col] = { type: initialSetup[1][col], color: 'white' };
        }
    }
    
    renderBoard() {
        const boardElement = document.getElementById('chessboard');
        boardElement.innerHTML = '';
        
        for (let row = 0; row < 8; row++) {
            for (let col = 0; col < 8; col++) {
                const square = document.createElement('div');
                square.className = `square ${(row + col) % 2 === 0 ? 'light' : 'dark'}`;
                square.dataset.row = row;
                square.dataset.col = col;
                
                const piece = this.board[row][col];
                if (piece) {
                    const pieceElement = document.createElement('span');
                    pieceElement.className = `piece ${piece.color}`;
                    pieceElement.textContent = this.pieces[piece.color][piece.type];
                    square.appendChild(pieceElement);
                }
                
                boardElement.appendChild(square);
            }
        }
    }
    
    setupEventListeners() {
        document.getElementById('chessboard').addEventListener('click', (e) => {
            const square = e.target.closest('.square');
            if (square && !this.isGameOver) {
                this.handleSquareClick(square);
            }
        });
        
        document.getElementById('reset-btn').addEventListener('click', () => {
            this.resetGame();
        });
        
        document.getElementById('undo-btn').addEventListener('click', () => {
            this.undoMove();
        });
    }
    
    handleSquareClick(square) {
        // Solo permitir clicks si es el turno del jugador humano (celeste/white) y la IA no está pensando
        if (this.currentPlayer !== 'white' || this.isAIThinking) return;
        
        const row = parseInt(square.dataset.row);
        const col = parseInt(square.dataset.col);
        
        if (this.selectedSquare) {
            // Intentar mover
            if (this.isValidMove(this.selectedSquare.row, this.selectedSquare.col, row, col)) {
                this.makeMove(this.selectedSquare.row, this.selectedSquare.col, row, col);
                this.clearSelection();
                this.switchPlayer();
                this.checkGameStatus();
                
                // Si ahora es turno de la IA, hacer que piense
                if (this.currentPlayer === 'black' && !this.isGameOver) {
                    this.makeAIMove();
                }
            } else {
                this.clearSelection();
                this.selectSquare(row, col);
            }
        } else {
            this.selectSquare(row, col);
        }
    }
    
    selectSquare(row, col) {
        const piece = this.board[row][col];
        // Solo permitir seleccionar piezas del jugador humano (celeste/white)
        if (piece && piece.color === 'white') {
            this.selectedSquare = { row, col };
            this.highlightSquare(row, col);
            this.showValidMoves(row, col);
        }
    }
    
    clearSelection() {
        this.selectedSquare = null;
        document.querySelectorAll('.square').forEach(square => {
            square.classList.remove('selected', 'valid-move', 'capture-move');
        });
    }
    
    highlightSquare(row, col) {
        const square = document.querySelector(`[data-row="${row}"][data-col="${col}"]`);
        square.classList.add('selected');
    }
    
    showValidMoves(row, col) {
        const piece = this.board[row][col];
        const validMoves = this.getValidMoves(row, col, piece);
        
        validMoves.forEach(([moveRow, moveCol]) => {
            const square = document.querySelector(`[data-row="${moveRow}"][data-col="${moveCol}"]`);
            if (this.board[moveRow][moveCol]) {
                square.classList.add('capture-move');
            } else {
                square.classList.add('valid-move');
            }
        });
    }
    
    getValidMoves(row, col, piece) {
        const moves = [];
        
        switch (piece.type) {
            case 'pawn':
                moves.push(...this.getPawnMoves(row, col, piece.color));
                break;
            case 'rook':
                moves.push(...this.getRookMoves(row, col));
                break;
            case 'knight':
                moves.push(...this.getKnightMoves(row, col));
                break;
            case 'bishop':
                moves.push(...this.getBishopMoves(row, col));
                break;
            case 'queen':
                moves.push(...this.getQueenMoves(row, col));
                break;
            case 'king':
                moves.push(...this.getKingMoves(row, col));
                break;
        }
        
        return moves.filter(([moveRow, moveCol]) => 
            this.isInBounds(moveRow, moveCol) && 
            (!this.board[moveRow][moveCol] || this.board[moveRow][moveCol].color !== piece.color)
        );
    }
    
    getPawnMoves(row, col, color) {
        const moves = [];
        const direction = color === 'white' ? -1 : 1;
        const startRow = color === 'white' ? 6 : 1;
        
        // Movimiento hacia adelante
        if (this.isInBounds(row + direction, col) && !this.board[row + direction][col]) {
            moves.push([row + direction, col]);
            
            // Movimiento doble desde posición inicial
            if (row === startRow && !this.board[row + 2 * direction][col]) {
                moves.push([row + 2 * direction, col]);
            }
        }
        
        // Capturas diagonales
        [-1, 1].forEach(colOffset => {
            const newRow = row + direction;
            const newCol = col + colOffset;
            if (this.isInBounds(newRow, newCol) && 
                this.board[newRow][newCol] && 
                this.board[newRow][newCol].color !== color) {
                moves.push([newRow, newCol]);
            }
        });
        
        return moves;
    }
    
    getRookMoves(row, col) {
        const moves = [];
        const directions = [[0, 1], [0, -1], [1, 0], [-1, 0]];
        
        directions.forEach(([rowDir, colDir]) => {
            for (let i = 1; i < 8; i++) {
                const newRow = row + i * rowDir;
                const newCol = col + i * colDir;
                
                if (!this.isInBounds(newRow, newCol)) break;
                
                if (this.board[newRow][newCol]) {
                    if (this.board[newRow][newCol].color !== this.board[row][col].color) {
                        moves.push([newRow, newCol]);
                    }
                    break;
                }
                
                moves.push([newRow, newCol]);
            }
        });
        
        return moves;
    }
    
    getKnightMoves(row, col) {
        const moves = [];
        const knightMoves = [
            [-2, -1], [-2, 1], [-1, -2], [-1, 2],
            [1, -2], [1, 2], [2, -1], [2, 1]
        ];
        
        knightMoves.forEach(([rowOffset, colOffset]) => {
            const newRow = row + rowOffset;
            const newCol = col + colOffset;
            
            if (this.isInBounds(newRow, newCol)) {
                moves.push([newRow, newCol]);
            }
        });
        
        return moves;
    }
    
    getBishopMoves(row, col) {
        const moves = [];
        const directions = [[1, 1], [1, -1], [-1, 1], [-1, -1]];
        
        directions.forEach(([rowDir, colDir]) => {
            for (let i = 1; i < 8; i++) {
                const newRow = row + i * rowDir;
                const newCol = col + i * colDir;
                
                if (!this.isInBounds(newRow, newCol)) break;
                
                if (this.board[newRow][newCol]) {
                    if (this.board[newRow][newCol].color !== this.board[row][col].color) {
                        moves.push([newRow, newCol]);
                    }
                    break;
                }
                
                moves.push([newRow, newCol]);
            }
        });
        
        return moves;
    }
    
    getQueenMoves(row, col) {
        return [...this.getRookMoves(row, col), ...this.getBishopMoves(row, col)];
    }
    
    getKingMoves(row, col) {
        const moves = [];
        const kingMoves = [
            [-1, -1], [-1, 0], [-1, 1],
            [0, -1],           [0, 1],
            [1, -1], [1, 0], [1, 1]
        ];
        
        kingMoves.forEach(([rowOffset, colOffset]) => {
            const newRow = row + rowOffset;
            const newCol = col + colOffset;
            
            if (this.isInBounds(newRow, newCol)) {
                moves.push([newRow, newCol]);
            }
        });
        
        return moves;
    }
    
    isInBounds(row, col) {
        return row >= 0 && row < 8 && col >= 0 && col < 8;
    }
    
    isValidMove(fromRow, fromCol, toRow, toCol) {
        const piece = this.board[fromRow][fromCol];
        if (!piece || piece.color !== this.currentPlayer) return false;
        
        const validMoves = this.getValidMoves(fromRow, fromCol, piece);
        return validMoves.some(([row, col]) => row === toRow && col === toCol);
    }
    
    makeMove(fromRow, fromCol, toRow, toCol) {
        const piece = this.board[fromRow][fromCol];
        const capturedPiece = this.board[toRow][toCol];
        
        // Guardar el estado para deshacer
        this.gameHistory.push({
            from: { row: fromRow, col: fromCol },
            to: { row: toRow, col: toCol },
            piece: { ...piece },
            capturedPiece: capturedPiece ? { ...capturedPiece } : null,
            currentPlayer: this.currentPlayer
        });
        
        // Verificar si se capturó un rey - GAME OVER inmediato
        if (capturedPiece && capturedPiece.type === 'king') {
            this.capturedPieces[capturedPiece.color].push(capturedPiece);
            this.renderCapturedPieces();
            
            // Mover la pieza
            this.board[toRow][toCol] = piece;
            this.board[fromRow][fromCol] = null;
            
            this.renderBoard();
            this.highlightLastMove(fromRow, fromCol, toRow, toCol);
            
            // Terminar el juego inmediatamente
            this.endGameByCapture(capturedPiece.color);
            return;
        }
        
        // Capturar pieza si existe (pero no es rey)
        if (capturedPiece) {
            this.capturedPieces[capturedPiece.color].push(capturedPiece);
            this.renderCapturedPieces();
        }
        
        // Mover la pieza
        this.board[toRow][toCol] = piece;
        this.board[fromRow][fromCol] = null;
        
        // Promoción de peón
        if (piece.type === 'pawn' && (toRow === 0 || toRow === 7)) {
            this.board[toRow][toCol] = { type: 'queen', color: piece.color };
        }
        
        this.renderBoard();
        this.highlightLastMove(fromRow, fromCol, toRow, toCol);
    }
    
    highlightLastMove(fromRow, fromCol, toRow, toCol) {
        setTimeout(() => {
            const fromSquare = document.querySelector(`[data-row="${fromRow}"][data-col="${fromCol}"]`);
            const toSquare = document.querySelector(`[data-row="${toRow}"][data-col="${toCol}"]`);
            
            fromSquare.classList.add('last-move');
            toSquare.classList.add('last-move');
            
            setTimeout(() => {
                fromSquare.classList.remove('last-move');
                toSquare.classList.remove('last-move');
            }, 2000);
        }, 100);
    }
    
    switchPlayer() {
        this.currentPlayer = this.currentPlayer === 'white' ? 'black' : 'white';
        document.getElementById('current-player').textContent = 
            `Turno: ${this.currentPlayer === 'white' ? 'Celestes' : 'Amarillas'}`;
    }
    
    checkGameStatus() {
        // Si el juego ya terminó por captura de rey, no hacer nada más
        if (this.isGameOver) return;
        
        const kingInCheck = this.isKingInCheck(this.currentPlayer);
        const hasValidMoves = this.hasValidMoves(this.currentPlayer);
        
        if (kingInCheck && !hasValidMoves) {
            this.isGameOver = true;
            const winner = this.currentPlayer === 'white' ? 'Amarillas' : 'Celestes';
            document.getElementById('game-status').textContent = `¡Jaque Mate! Ganan las ${winner}`;
        } else if (!hasValidMoves) {
            this.isGameOver = true;
            document.getElementById('game-status').textContent = '¡Tablas por ahogado!';
        } else if (kingInCheck) {
            document.getElementById('game-status').textContent = '¡Jaque!';
        } else {
            document.getElementById('game-status').textContent = '';
        }
    }
    
    endGameByCapture(capturedKingColor) {
        this.isGameOver = true;
        const winner = capturedKingColor === 'white' ? 'Amarillas' : 'Celestes';
        const winnerIcon = capturedKingColor === 'white' ? '🟡' : '🔵';
        
        document.getElementById('game-status').textContent = `${winnerIcon} ¡REY CAPTURADO! Ganan las ${winner}`;
        
        // Agregar efecto visual de victoria
        const statusElement = document.getElementById('game-status');
        statusElement.style.fontSize = '1.2em';
        statusElement.style.fontWeight = 'bold';
        statusElement.style.color = capturedKingColor === 'white' ? '#FFD700' : '#4682B4';
        statusElement.style.textShadow = '2px 2px 4px rgba(0,0,0,0.5)';
    }
    
    isKingInCheck(color) {
        // Encontrar el rey
        let kingPos = null;
        for (let row = 0; row < 8; row++) {
            for (let col = 0; col < 8; col++) {
                const piece = this.board[row][col];
                if (piece && piece.type === 'king' && piece.color === color) {
                    kingPos = { row, col };
                    break;
                }
            }
            if (kingPos) break;
        }
        
        if (!kingPos) return false;
        
        // Verificar si alguna pieza enemiga puede atacar al rey
        const enemyColor = color === 'white' ? 'black' : 'white';
        for (let row = 0; row < 8; row++) {
            for (let col = 0; col < 8; col++) {
                const piece = this.board[row][col];
                if (piece && piece.color === enemyColor) {
                    const validMoves = this.getValidMoves(row, col, piece);
                    if (validMoves.some(([moveRow, moveCol]) => 
                        moveRow === kingPos.row && moveCol === kingPos.col)) {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    hasValidMoves(color) {
        for (let row = 0; row < 8; row++) {
            for (let col = 0; col < 8; col++) {
                const piece = this.board[row][col];
                if (piece && piece.color === color) {
                    const validMoves = this.getValidMoves(row, col, piece);
                    if (validMoves.length > 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    renderCapturedPieces() {
        const whiteCaptured = document.getElementById('captured-white-pieces');
        const blackCaptured = document.getElementById('captured-black-pieces');
        
        whiteCaptured.innerHTML = '';
        blackCaptured.innerHTML = '';
        
        this.capturedPieces.white.forEach(piece => {
            const pieceElement = document.createElement('span');
            pieceElement.className = 'captured-piece';
            pieceElement.textContent = this.pieces[piece.color][piece.type];
            whiteCaptured.appendChild(pieceElement);
        });
        
        this.capturedPieces.black.forEach(piece => {
            const pieceElement = document.createElement('span');
            pieceElement.className = 'captured-piece';
            pieceElement.textContent = this.pieces[piece.color][piece.type];
            blackCaptured.appendChild(pieceElement);
        });
    }
    
    undoMove() {
        if (this.gameHistory.length === 0) return;
        
        const lastMove = this.gameHistory.pop();
        
        // Restaurar la pieza movida
        this.board[lastMove.from.row][lastMove.from.col] = lastMove.piece;
        this.board[lastMove.to.row][lastMove.to.col] = lastMove.capturedPiece;
        
        // Restaurar pieza capturada
        if (lastMove.capturedPiece) {
            const capturedArray = this.capturedPieces[lastMove.capturedPiece.color];
            capturedArray.pop();
            this.renderCapturedPieces();
        }
        
        // Restaurar turno
        this.currentPlayer = lastMove.currentPlayer;
        document.getElementById('current-player').textContent = 
            `Turno: ${this.currentPlayer === 'white' ? 'Celestes' : 'Amarillas'}`;
        
        this.isGameOver = false;
        document.getElementById('game-status').textContent = '';
        
        this.clearSelection();
        this.renderBoard();
    }
    
    resetGame() {
        this.board = [];
        this.currentPlayer = 'white';
        this.selectedSquare = null;
        this.gameHistory = [];
        this.capturedPieces = { white: [], black: [] };
        this.isGameOver = false;
        
        document.getElementById('current-player').textContent = 'Turno: Celestes';
        
        // Limpiar estilos del status
        const statusElement = document.getElementById('game-status');
        statusElement.textContent = '';
        statusElement.style.fontSize = '';
        statusElement.style.fontWeight = '';
        statusElement.style.color = '';
        statusElement.style.textShadow = '';
        
        this.initializeBoard();
        this.renderBoard();
        this.renderCapturedPieces();
        this.clearSelection();
    }
    
    // IA para el equipo amarillo
    makeAIMove() {
        this.isAIThinking = true;
        document.getElementById('game-status').textContent = '🤖 La IA está pensando...';
        
        setTimeout(() => {
            const bestMove = this.findBestMove();
            if (bestMove) {
                this.makeMove(bestMove.fromRow, bestMove.fromCol, bestMove.toRow, bestMove.toCol);
                this.switchPlayer();
                this.checkGameStatus();
            }
            this.isAIThinking = false;
        }, 1000); // Delay de 1 segundo para que se sienta natural
    }
    
    findBestMove() {
        const allMoves = this.getAllPossibleMoves('black');
        if (allMoves.length === 0) return null;
        
        // Priorizar movimientos: capturas > movimientos seguros > movimientos aleatorios
        const captureMoves = allMoves.filter(move => 
            this.board[move.toRow][move.toCol] !== null
        );
        
        if (captureMoves.length > 0) {
            // Si hay capturas, elegir la de mayor valor
            captureMoves.sort((a, b) => {
                const valueA = this.getPieceValue(this.board[a.toRow][a.toCol]);
                const valueB = this.getPieceValue(this.board[b.toRow][b.toCol]);
                return valueB - valueA;
            });
            return captureMoves[0];
        }
        
        // Si no hay capturas, hacer un movimiento estratégico
        const safeMoves = allMoves.filter(move => 
            !this.wouldBeInCheckAfterMove(move.fromRow, move.fromCol, move.toRow, move.toCol)
        );
        
        if (safeMoves.length > 0) {
            return safeMoves[Math.floor(Math.random() * safeMoves.length)];
        }
        
        // Como último recurso, cualquier movimiento válido
        return allMoves[Math.floor(Math.random() * allMoves.length)];
    }
    
    getAllPossibleMoves(color) {
        const moves = [];
        for (let row = 0; row < 8; row++) {
            for (let col = 0; col < 8; col++) {
                const piece = this.board[row][col];
                if (piece && piece.color === color) {
                    const validMoves = this.getValidMoves(row, col, piece);
                    validMoves.forEach(([toRow, toCol]) => {
                        moves.push({ fromRow: row, fromCol: col, toRow, toCol });
                    });
                }
            }
        }
        return moves;
    }
    
    getPieceValue(piece) {
        if (!piece) return 0;
        const values = {
            'pawn': 1,
            'knight': 3,
            'bishop': 5,
            'rook': 5,
            'queen': 20,
            'king': 1000
        };
        return values[piece.type] || 0;
    }
    
    wouldBeInCheckAfterMove(fromRow, fromCol, toRow, toCol) {
        // Simular el movimiento
        const originalPiece = this.board[toRow][toCol];
        const movingPiece = this.board[fromRow][fromCol];
        
        this.board[toRow][toCol] = movingPiece;
        this.board[fromRow][fromCol] = null;
        
        const inCheck = this.isKingInCheck('black');
        
        // Restaurar el estado
        this.board[fromRow][fromCol] = movingPiece;
        this.board[toRow][toCol] = originalPiece;
        
        return inCheck;
    }
}

// Inicializar el juego cuando se carga la página
document.addEventListener('DOMContentLoaded', () => {
    new ChessGame();
}); 