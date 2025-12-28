"""
Data Manager Service
Handles Pandas data processing logic and session management.
"""

import io
import json
import uuid
from typing import Any

import pandas as pd
import plotly.express as px

# Global dictionary to store active sessions by ID
sessions: dict[str, "DataSession"] = {}


class DataSession:
    """
    Manages a user's uploaded data session.
    Holds both the original dataframe (backup) and the active dataframe (modified by queries).
    """

    def __init__(self, file_path: str):
        """
        Initialize a data session by loading a CSV or Excel file.

        Args:
            file_path: Path to the uploaded file (CSV or Excel).
        """
        # Determine file type and load accordingly
        if file_path.endswith(".csv"):
            self.df_original = pd.read_csv(file_path)
        elif file_path.endswith((".xlsx", ".xls")):
            self.df_original = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")

        # Active dataframe starts as a copy of the original
        self.df_active = self.df_original.copy()

        # History for undo functionality
        self.history: list[pd.DataFrame] = []

        # Conversation history for context-aware responses
        self.conversation_history: list[dict] = []

    def add_message(self, role: str, content: str) -> None:
        """
        Add a message to the conversation history.
        Keeps only the last 10 messages to save context tokens.

        Args:
            role: Either "user" or "assistant"
            content: The message content
        """
        self.conversation_history.append({"role": role, "content": content})
        # Keep only the last 10 messages
        if len(self.conversation_history) > 10:
            self.conversation_history = self.conversation_history[-10:]

    def get_chat_history_str(self) -> str:
        """
        Format the conversation history as a string for the LLM.

        Returns:
            Formatted string of the conversation history.
        """
        if not self.conversation_history:
            return "No previous conversation."

        lines = []
        for msg in self.conversation_history:
            role_label = "User" if msg["role"] == "user" else "Assistant"
            lines.append(f"{role_label}: {msg['content']}")

        return "\n".join(lines)

    def get_preview(self) -> dict[str, Any]:
        """
        Get a preview of the active dataframe.

        Returns:
            Dictionary containing columns, dtypes, and first 5 rows.
        """
        return {
            "columns": self.df_active.columns.tolist(),
            "dtypes": self.df_active.dtypes.astype(str).to_dict(),
            "rows": self.df_active.head(5).to_dict(orient="records"),
        }

    def reset(self) -> None:
        """
        Revert the active dataframe to the original state.
        Clears the history and conversation.
        """
        self.df_active = self.df_original.copy()
        self.history.clear()
        self.conversation_history.clear()

    def undo(self) -> bool:
        """
        Undo the last dataframe modification.

        Returns:
            True if undo was successful, False if there's nothing to undo.
        """
        if not self.history:
            return False

        # Pop the last state and restore it
        last_state = self.history.pop()
        self.df_active = last_state
        return True

    def execute_code(self, code_str: str) -> dict[str, Any]:
        """
        Execute LLM-generated Python code against the active dataframe.

        CRITICAL: This method runs dynamically generated code.
        The code has access to the dataframe (df), pandas (pd), and plotly.express (px).

        Args:
            code_str: Python code string to execute.

        Returns:
            Dictionary with status, result text, and plot_json (if any).
        """
        # Create execution environment
        local_env: dict[str, Any] = {
            "df": self.df_active,
            "pd": pd,
            "px": px,
        }

        result: dict[str, Any] = {
            "status": "success",
            "result": None,
            "plot_json": None,
        }

        try:
            # Capture stdout for any print statements
            import sys
            from io import StringIO

            old_stdout = sys.stdout
            sys.stdout = captured_output = StringIO()

            # Execute the code
            exec(code_str, globals(), local_env)

            # Restore stdout
            sys.stdout = old_stdout
            output_text = captured_output.getvalue()

            # Check if the dataframe was modified
            if "df" in local_env:
                new_df = local_env["df"]
                if isinstance(new_df, pd.DataFrame) and not new_df.equals(self.df_active):
                    # Save current state to history before updating
                    self.history.append(self.df_active.copy())
                    self.df_active = new_df

            # Check for Plotly figure
            if "fig" in local_env:
                fig = local_env["fig"]
                if hasattr(fig, "to_json"):
                    result["plot_json"] = fig.to_json()

            # Check for a result variable
            if "result" in local_env:
                result["result"] = str(local_env["result"])
            elif output_text:
                result["result"] = output_text

        except Exception as e:
            result["status"] = "error"
            result["result"] = f"Error executing code: {str(e)}"

        return result


def get_session(session_id: str) -> DataSession | None:
    """
    Retrieve a session object from the global sessions dictionary.

    Args:
        session_id: The unique session identifier.

    Returns:
        The DataSession object if found, None otherwise.
    """
    return sessions.get(session_id)
