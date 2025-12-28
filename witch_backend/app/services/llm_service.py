"""
LLM Service
Handles interaction with OpenAI API for code generation.
"""

import re

from openai import AsyncOpenAI

from app.core.config import settings


class LLMClient:
    """
    Client for interacting with OpenAI's LLM API.
    Generates Python code for data analysis tasks.
    """

    def __init__(self):
        """Initialize the LLM client with lazy loading."""
        self._client: AsyncOpenAI | None = None
        self.model = "gpt-4o"

    @property
    def client(self) -> AsyncOpenAI:
        """Lazy initialization of the OpenAI client."""
        if self._client is None:
            api_key = settings.OPENAI_API_KEY
            if not api_key:
                raise ValueError(
                    "OPENAI_API_KEY is not set. Please set it in .env file or environment variables."
                )
            self._client = AsyncOpenAI(api_key=api_key)
        return self._client

    def _clean_code(self, text: str) -> str:
        """
        Clean the LLM response by stripping markdown formatting.

        Args:
            text: Raw text response from the LLM.

        Returns:
            Clean Python code without markdown backticks.
        """
        # Remove markdown code blocks (```python ... ``` or ``` ... ```)
        code = text.strip()

        # Match ```python or ```py or just ```
        pattern = r"```(?:python|py)?\s*\n?(.*?)\n?```"
        match = re.search(pattern, code, re.DOTALL)

        if match:
            code = match.group(1).strip()

        # Remove redundant import statements and dangerous calls
        lines = code.split("\n")
        cleaned_lines = []
        for line in lines:
            # Skip redundant imports that are already in the execution environment
            if re.match(r"^\s*import\s+pandas(\s+as\s+pd)?\s*$", line):
                continue
            if re.match(r"^\s*import\s+plotly\.express(\s+as\s+px)?\s*$", line):
                continue
            if re.match(r"^\s*from\s+plotly\s+import\s+express(\s+as\s+px)?\s*$", line):
                continue
            # Remove fig.show() calls - they open browser windows!
            if re.match(r"^\s*fig\.show\(\s*\)\s*$", line):
                continue
            # Remove any .show() calls on figures
            if ".show()" in line and "fig" in line:
                continue
            cleaned_lines.append(line)

        return "\n".join(cleaned_lines).strip()

    async def generate_code(self, data_preview: str, user_query: str, chat_history: str = "") -> str:
        """
        Generate Python code based on user query and data context.

        Args:
            data_preview: String representation of the dataframe preview.
            user_query: The user's natural language query.
            chat_history: Previous conversation history for context.

        Returns:
            Clean Python code to execute.
        """
        system_prompt = """You are a strict Python Data Scientist assistant.
You have access to a pandas DataFrame called `df` that is already loaded in memory.
You also have access to `pd` (pandas) and `px` (plotly.express).

CONTEXT AWARENESS:
- Use the HISTORY section to understand references like "it", "them", "those", "that", "the result", etc.
- If the user asks a follow-up question (e.g., "how many?" or "show me a chart of that"), refer to the previous context.
- If the user says "filter that" or "keep only those", apply the filter based on the previous query's subject.

=== DATA SAFETY RULES (CRITICAL) ===

SAFETY LATCH - DO NOT MODIFY df FOR EXPLORATORY QUESTIONS:
- If the user asks "How can we...", "What if...", "Suggest...", "Which...", "What would happen if...", or any exploratory/hypothetical question:
  → Do NOT modify `df`
  → Calculate and report the answer in `result`
  → Example: "How can we cut costs?" → Analyze and report potential savings, but do NOT delete any rows

EXPLICIT MODIFICATION - ONLY modify df when user uses imperative commands:
- ONLY modify `df` if the user explicitly uses action verbs like: "Filter", "Remove", "Delete", "Keep", "Drop", "Clean", "Update", "Change", "Set", "Apply"
- Example SAFE (no modification): "How many low-value transactions are there?" → Just count and report
- Example MODIFY (explicit command): "Remove all low-value transactions" → Filter df and confirm

HYPOTHETICAL ANALYSIS - Use temporary copies for "What if" scenarios:
- For "What if we removed...", "If we filtered...", "Assuming we deleted...":
  → Create a temporary copy: temp_df = df.copy()
  → Perform the analysis on temp_df
  → Report the hypothetical result in `result`
  → Do NOT overwrite `df`
- Example: "What if we removed orders under $50?"
  → temp_df = df[df['Amount'] >= 50]
  → result = f"If we removed orders under $50, {len(df) - len(temp_df):,} rows would be removed, leaving {len(temp_df):,} rows."

=== END DATA SAFETY RULES ===

RULES:
1. NEVER load or read files. The dataframe `df` is already available.
2. Use `plotly.express` as `px` for ALL charts and visualizations.
3. If you filter, transform, or modify the data (AND the user explicitly requested it), assign the result back to `df`.
4. If you create a plot, assign it to `fig`. NEVER call fig.show() - this is handled automatically.
5. Do NOT use matplotlib or seaborn. Use plotly.express only.
6. Do NOT include import statements for pandas or plotly.express.
7. Write clean, executable Python code only. No explanations.
8. NEVER call fig.show(), plt.show(), or any .show() method. The figure is rendered automatically.

CRITICAL OUTPUT FORMATTING RULES:
8. ALWAYS assign a human-readable string to `result` for ANY text-based answer.
   - BAD:  result = df['Amount'].sum()
   - GOOD: result = f"The total amount is {int(df['Amount'].sum()):,}"

9. When computing statistics, format them nicely in natural language:
   - BAD:  result = df['Price'].mean()
   - GOOD: result = f"The average price is ${float(df['Price'].mean()):.2f}"

10. ALWAYS convert numpy types to Python types in f-strings using int(), float(), or str():
    - Use int() for counts, sums of integers
    - Use float() for means, percentages, decimals
    - Use :, for thousands separators, :.2f for 2 decimal places

11. If you modify `df` (filter, sort, drop, transform), you MUST also set a confirmation message:
    - Example: df = df[df['Status'] == 'Active']
               result = f"Filtered to Active status only. {len(df):,} rows remaining."
    - Example: df = df.sort_values('Date', ascending=False)
               result = f"Sorted by Date in descending order. {len(df):,} rows."
    - Example: df = df.dropna()
               result = f"Removed rows with missing values. {len(df):,} rows remaining."

12. For value counts or distributions, format as a readable list:
    - Example: counts = df['Category'].value_counts()
               result = "Category distribution:\\n" + "\\n".join([f"  • {cat}: {int(count):,}" for cat, count in counts.items()])

OUTPUT FORMAT:
Return ONLY the Python code. No markdown, no explanations, no comments unless necessary."""

        # Build user message with history context
        history_section = ""
        if chat_history and chat_history != "No previous conversation.":
            history_section = f"""CONVERSATION HISTORY:
{chat_history}

"""

        user_message = f"""{history_section}CURRENT DATAFRAME PREVIEW:
{data_preview}

CURRENT USER QUERY: {user_query}

Generate the Python code to accomplish this task. Use the conversation history to resolve any references."""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
        )

        raw_code = response.choices[0].message.content or ""
        return self._clean_code(raw_code)

    async def fix_code(self, broken_code: str, error_message: str, data_preview: str, chat_history: str = "") -> str:
        """
        Attempt to fix broken code based on the error message.

        Args:
            broken_code: The code that failed to execute.
            error_message: The error message from the failed execution.
            data_preview: String representation of the dataframe preview.
            chat_history: Previous conversation history for context.

        Returns:
            Fixed Python code.
        """
        system_prompt = """You are a Python debugging expert.
Your task is to fix broken pandas/plotly code.

CONTEXT AWARENESS:
- Use the HISTORY section to understand what the user was trying to accomplish.
- Resolve references like "it", "them", "those" from the conversation context.

RULES:
1. The dataframe `df` is already loaded. NEVER load files.
2. `pd` (pandas) and `px` (plotly.express) are already available.
3. If modifying data, assign back to `df`.
4. If creating a plot, assign to `fig`.
5. Do NOT include import statements.
6. Return ONLY the fixed Python code. No explanations.

CRITICAL OUTPUT FORMATTING RULES:
7. ALWAYS assign a human-readable string to `result` for text answers.
   - BAD:  result = df['Amount'].sum()
   - GOOD: result = f"The total amount is {int(df['Amount'].sum()):,}"

8. ALWAYS convert numpy types to Python types using int(), float(), or str().

9. If modifying `df`, ALWAYS set a confirmation message:
   - Example: result = f"Operation complete. {len(df):,} rows remaining."

10. Format numbers nicely: use :, for thousands, :.2f for decimals."""

        # Build user message with history context
        history_section = ""
        if chat_history and chat_history != "No previous conversation.":
            history_section = f"""CONVERSATION HISTORY:
{chat_history}

"""

        user_message = f"""{history_section}The following code failed with an error.

BROKEN CODE:
```python
{broken_code}
```

ERROR MESSAGE:
{error_message}

DATAFRAME PREVIEW:
{data_preview}

Fix the code and return only the corrected Python code."""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
        )

        raw_code = response.choices[0].message.content or ""
        return self._clean_code(raw_code)


    async def generate_sql(self, schema_context: str, user_question: str, chat_history: str = "") -> str:
        """
        Generate a SQL query based on user question and database schema.

        Args:
            schema_context: Database schema description.
            user_question: The user's natural language question.
            chat_history: Previous conversation history for context.

        Returns:
            SQL query string.
        """
        system_prompt = """You are a PostgreSQL Expert. Given the database schema below, write a valid SQL query to answer the user's question.

=== CRITICAL: CONVERSATION CONTEXT AWARENESS ===
You MUST pay close attention to the CONVERSATION HISTORY to understand which table the user is currently working with.

TABLE CONTEXT RULES:
1. If the user previously explored a specific table (viewed its columns, queried its data), and then asks a follow-up question like "how many...", "show me...", "count...", "what about..." - they are referring to THAT SAME TABLE.

2. NEVER switch to a different table unless the user EXPLICITLY mentions a new table name.

3. Look for these patterns in the history to identify the "current table":
   - "show me columns of TABLE_NAME" → current table is TABLE_NAME
   - "SELECT * FROM TABLE_NAME" in previous SQL → current table is TABLE_NAME
   - "what is the TYPE column" after exploring TABLE_NAME → still TABLE_NAME

4. When the user asks "how many X do we have?" after exploring a table's column values:
   - Query THAT SAME TABLE, not a similarly named one
   - Example flow:
     * User: "show columns of collection_clean" → collection_clean
     * User: "what are the types?" → SELECT DISTINCT type FROM collection_clean
     * User: "how many Основной долг?" → SELECT COUNT(*) FROM collection_clean WHERE type = 'Основной долг'
   - DO NOT switch to dm_historical_soft_collection or any other table!

=== END CONTEXT RULES ===

=== INTERPRETATION QUESTIONS - DO NOT QUERY ===
When the user asks for EXPLANATION or INTERPRETATION (not data), respond with a special format.

RECOGNIZE THESE PATTERNS:
- "what is it?" / "what does it mean?" / "can you explain?" / "tell me in few words"
- "what is this column for?" / "what does X represent?"
- "describe this" / "summarize" / "interpret"
- "in simple terms" / "briefly explain"

FOR INTERPRETATION QUESTIONS:
Return this exact format (NOT a SQL query):
INTERPRET: [Your analysis based on the column name, data types, and previous query results]

Example:
- User previously saw Column1 with values like 1781920, 1781921, 1781922...
- User asks: "what is it can you tell me in few words?"
- Response: INTERPRET: Column1 appears to be a sequential numeric ID, likely an auto-incrementing primary key or account identifier.

=== END INTERPRETATION RULES ===

=== SMART DATA EXPLORATION ===
When exploring column contents, NEVER return all rows from large tables!

FOR "what is in column X" or "show me column X":
- Use LIMIT 10-20 for samples
- Or use SELECT DISTINCT with LIMIT for unique values
- Or use COUNT + GROUP BY for value distribution

GOOD EXAMPLES:
- SELECT DISTINCT "Column1" FROM table LIMIT 20;
- SELECT "Column1", COUNT(*) as cnt FROM table GROUP BY "Column1" ORDER BY cnt DESC LIMIT 10;
- SELECT "Column1" FROM table LIMIT 15;

BAD (NEVER DO THIS):
- SELECT "Column1" FROM table; (no LIMIT = returns millions of rows!)
- SELECT DISTINCT "Column1" FROM table; (no LIMIT on large tables!)

=== END SMART EXPLORATION ===

SQL RULES:
1. Return ONLY the SQL query (or INTERPRET: message). No markdown, no explanations, no backticks.
2. Use the provided table and column names EXACTLY as specified.
3. If the user asks for "recent", "latest", or "top N", use ORDER BY with LIMIT.
4. Use proper JOINs when referencing multiple tables.
5. Be mindful of NULL values - use COALESCE or IS NOT NULL where appropriate.
6. For text searches, use ILIKE for case-insensitive matching.
7. Always use table aliases for clarity in JOINs.
8. Format dates appropriately for PostgreSQL.
9. ALWAYS use LIMIT when exploring data unless user explicitly wants all rows.

SCHEMA:
{schema_context}

OUTPUT:
Return ONLY the raw SQL query OR an INTERPRET: message. No markdown code blocks."""

        # Build user message with history context
        history_section = ""
        if chat_history and chat_history.strip():
            history_section = f"""CONVERSATION HISTORY:
{chat_history}

"""

        user_message = f"""{history_section}DATABASE SCHEMA:
{schema_context}

USER QUESTION: {user_question}

Generate the SQL query to answer this question."""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt.format(schema_context=schema_context)},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
        )

        raw_sql = response.choices[0].message.content or ""
        return self._clean_sql(raw_sql)

    async def fix_sql(self, broken_sql: str, error_message: str, schema_context: str) -> str:
        """
        Attempt to fix a broken SQL query.

        Args:
            broken_sql: The SQL query that failed.
            error_message: The error message from the database.
            schema_context: Database schema description.

        Returns:
            Fixed SQL query string.
        """
        system_prompt = """You are a PostgreSQL debugging expert.
Your task is to fix broken SQL queries.

RULES:
1. Return ONLY the fixed SQL query. No markdown, no explanations.
2. Use the provided table and column names EXACTLY as specified.
3. Fix syntax errors, missing quotes, incorrect column names, etc.
4. Ensure proper JOINs and table aliases.

OUTPUT:
Return ONLY the raw SQL query. No markdown code blocks, no explanations."""

        user_message = f"""The following SQL query failed with an error.

BROKEN SQL:
{broken_sql}

ERROR MESSAGE:
{error_message}

DATABASE SCHEMA:
{schema_context}

Fix the SQL query and return only the corrected SQL."""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0.1,
        )

        raw_sql = response.choices[0].message.content or ""
        return self._clean_sql(raw_sql)

    def _clean_sql(self, text: str) -> str:
        """
        Clean SQL response by removing markdown formatting.

        Args:
            text: Raw text response from the LLM.

        Returns:
            Clean SQL query.
        """
        sql = text.strip()

        # Remove markdown code blocks
        pattern = r"```(?:sql)?\s*\n?(.*?)\n?```"
        match = re.search(pattern, sql, re.DOTALL | re.IGNORECASE)
        if match:
            sql = match.group(1).strip()

        # Remove any leading/trailing quotes
        sql = sql.strip('"\'')

        return sql


    async def suggest_features_llm(
        self, 
        schema_context: str, 
        target_goal: str, 
        table_name: str,
        grouping_column: str | None = None
    ) -> str:
        """
        Use LLM to suggest ML features based on schema and target.

        Args:
            schema_context: Database schema description.
            target_goal: What the user wants to predict.
            table_name: The table to generate features from.
            grouping_column: The column to group by (e.g., client_id).

        Returns:
            JSON string with feature suggestions.
        """
        grouping_info = ""
        if grouping_column:
            grouping_info = f"\nGROUPING COLUMN: {grouping_column} (all aggregations will be grouped by this column)"
        
        system_prompt = """You are an expert Machine Learning Feature Engineer specializing in Banking and Finance.

Given a database schema and a prediction target, suggest 5-7 high-impact features that can be derived from the data.

=== CRITICAL SQL RULES ===

1. **VARCHAR DATE COLUMNS**: Many date columns are stored as VARCHAR/TEXT, not DATE type!
   - ALWAYS cast to DATE before date arithmetic: "column"::DATE
   - For recency: EXTRACT(DAY FROM AGE(CURRENT_DATE, MAX("date_column"::DATE)))
   - For duration: EXTRACT(DAY FROM AGE(MAX("date_column"::DATE), MIN("date_column"::DATE)))
   - NEVER use: DATE_PART('day', CURRENT_DATE - TO_DATE(...))

2. **AGGREGATION TEMPLATES**: Use these exact patterns:
   - Average: COALESCE(AVG("column"), 0)
   - Sum: COALESCE(SUM("column"), 0)
   - Max: COALESCE(MAX("column"), 0)
   - Min: COALESCE(MIN("column"), 0)
   - Count: COUNT(*)
   - Count non-null: COUNT("column")
   - Stddev: COALESCE(STDDEV("column"), 0)

3. **RECENCY FEATURES** (days since last event):
   - COALESCE(EXTRACT(DAY FROM AGE(CURRENT_DATE, MAX("date_column"::DATE))), 9999)::INTEGER

4. **DURATION FEATURES** (span between first and last):
   - COALESCE(EXTRACT(DAY FROM AGE(MAX("date_column"::DATE), MIN("date_column"::DATE))), 0)::INTEGER

5. **CATEGORICAL MODE** (most frequent value):
   - MODE() WITHIN GROUP (ORDER BY "column")
   - Do NOT use subqueries for mode!

6. **NULL HANDLING**: Always wrap in COALESCE with sensible defaults:
   - Numeric: COALESCE(..., 0)
   - Dates/recency: COALESCE(..., 9999) for "very old"
   - Counts: COUNT() doesn't need COALESCE

7. **COLUMN NAMES**: Always use double quotes: "column_name"

=== END SQL RULES ===

FEATURE TYPES TO CONSIDER:
- aggregation: SUM, AVG, MAX, MIN, STDDEV of numeric columns
- recency: Days since last activity (critical for churn!)
- frequency: COUNT of events/records
- duration: Time span of relationship (tenure)
- categorical: MODE, distinct count
- ratio: Derived ratios between columns

OUTPUT FORMAT:
Return a valid JSON array with this structure:
[
  {
    "name": "feature_name_in_snake_case",
    "column": "source_column_name",
    "logic": "Human readable explanation",
    "sql_template": "COALESCE(AVG(\"column\"), 0)",
    "type": "aggregation|recency|frequency|duration|categorical|ratio",
    "relevance": "critical|high|medium",
    "description": "Why this feature is useful for the target"
  }
]

IMPORTANT:
- Return ONLY the JSON array, no markdown, no explanations.
- Each sql_template must be a SINGLE expression that can go in a SELECT clause.
- Do NOT include GROUP BY in the sql_template - that's handled separately."""

        user_message = f"""TABLE: {table_name}
{grouping_info}
SCHEMA:
{schema_context}

TARGET GOAL: {target_goal}

Suggest 5-7 high-impact features for predicting {target_goal}. Return only the JSON array."""

        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            temperature=0.2,
        )

        raw_response = response.choices[0].message.content or "[]"
        
        # Clean markdown if present
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            pattern = r"```(?:json)?\s*\n?(.*?)\n?```"
            match = re.search(pattern, cleaned, re.DOTALL)
            if match:
                cleaned = match.group(1).strip()
        
        return cleaned



llm_client = LLMClient()
