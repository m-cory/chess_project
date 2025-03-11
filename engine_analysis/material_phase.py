import chess
import logging

logger = logging.getLogger(__name__)

PIECE_VALUES = {
    chess.PAWN: 1,
    chess.KNIGHT: 3,
    chess.BISHOP: 3,
    chess.ROOK: 5,
    chess.QUEEN: 9
}

def total_material_on_board(board: chess.Board) -> int:
    total = 0
    for piece_type, value in PIECE_VALUES.items():
        count = len(board.pieces(piece_type, chess.WHITE)) + len(board.pieces(piece_type, chess.BLACK))
        total += count * value
    return total

def get_phase_by_material(board: chess.Board, opening_threshold: int, endgame_threshold: int) -> str:
    mat = total_material_on_board(board)
    if mat >= opening_threshold:
        return "opening"
    elif mat <= endgame_threshold:
        return "endgame"
    else:
        return "midgame"
