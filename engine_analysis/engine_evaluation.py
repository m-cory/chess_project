import io
import chess
import chess.pgn
import chess.engine
import logging

from .material_phase import get_phase_by_material

logger = logging.getLogger(__name__)

def which_player_moved(board: chess.Board, user_side: str) -> str:
    """
    Returns 'user' if 'user_side' matches the side to move, else 'opponent'.
    """
    side_to_move_white = board.turn  # True if White to move
    if side_to_move_white and user_side == "white":
        return "user"
    if (not side_to_move_white) and user_side == "black":
        return "user"
    return "opponent"

def analyze_game_user_opponent(
    pgn_text: str,
    user_side: str,        # "white" or "black"
    engine_path: str,
    depth: int,
    blunder_threshold: int,
    opening_threshold: int,
    endgame_threshold: int
) -> dict:
    result = {
        "user_blunders_opening": 0,     "user_blunders_midgame": 0,     "user_blunders_endgame": 0,
        "opponent_blunders_opening": 0, "opponent_blunders_midgame": 0, "opponent_blunders_endgame": 0,

        "user_acpl_opening": 0.0,       "user_acpl_midgame": 0.0,       "user_acpl_endgame": 0.0,
        "opponent_acpl_opening": 0.0,   "opponent_acpl_midgame": 0.0,   "opponent_acpl_endgame": 0.0,

        "moves_opening": 0,
        "moves_midgame": 0,
        "moves_endgame": 0,

        "user_opening_moves": 0,    "user_midgame_moves": 0,    "user_endgame_moves": 0,
        "opponent_opening_moves": 0,"opponent_midgame_moves": 0,"opponent_endgame_moves": 0,

        # Keep track of the analysis parameters if you'd like
        "analysis_depth": depth,
        "analysis_blunder_threshold": blunder_threshold,
        "analysis_opening_threshold": opening_threshold,
        "analysis_endgame_threshold": endgame_threshold,
    }

    game = chess.pgn.read_game(io.StringIO(pgn_text))
    if not game:
        logger.warning("Could not parse PGN.")
        return result

    try:
        engine = chess.engine.SimpleEngine.popen_uci(engine_path)
    except Exception as e:
        logger.error(f"Error launching engine: {e}")
        return result

    board = game.board()
    for move in game.mainline_moves():
        mover = which_player_moved(board, user_side)
        phase = get_phase_by_material(board, opening_threshold, endgame_threshold)

        info_before = engine.analyse(board, limit=chess.engine.Limit(depth=depth))
        eval_before = info_before["score"].pov(chess.WHITE).score()

        board.push(move)

        info_after = engine.analyse(board, limit=chess.engine.Limit(depth=depth))
        eval_after = info_after["score"].pov(chess.WHITE).score()

        if eval_before is not None and eval_after is not None:
            delta = eval_after - eval_before
            # Blunder
            if delta <= -blunder_threshold:
                bk = f"{mover}_blunders_{phase}"
                result[bk] += 1

            # ACPL
            cp_loss = -delta if delta < 0 else 0
            ak = f"{mover}_acpl_{phase}"
            mk = f"{mover}_{phase}_moves"
            result[ak] += cp_loss
            result[mk] += 1

        result[f"moves_{phase}"] += 1

    engine.quit()

    # Compute final ACPL
    for side in ["user", "opponent"]:
        for ph in ["opening", "midgame", "endgame"]:
            ac_key = f"{side}_acpl_{ph}"
            count_key = f"{side}_{ph}_moves"
            if result[count_key] > 0:
                result[ac_key] = result[ac_key] / result[count_key]

    return result
