// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! A bounded reader for the two DuckDB scalar SQL grammars the `vgi` extension
//! accepts in catalog metadata, so the SDK can refuse what ATTACH would refuse:
//!
//! - **CHECK** (`ParseNumericCheck` in the extension's
//!   `vgi_constraint_contract.cpp`): exactly one comparison
//!   (`=`, `!=`/`<>`, `<`, `>`, `<=`, `>=`) whose two operands are numeric
//!   literals or unqualified references to primitive signed numeric columns.
//! - **Generated columns** (`Parse(..., generated = true)` in
//!   `vgi_column_expression.cpp`): literals of NULL / BOOLEAN / integer /
//!   VARCHAR type, unqualified column references, `CAST` to those types,
//!   comparisons, `AND` / `OR` / `NOT`, `IS [NOT] NULL`, `COALESCE`, and the
//!   functions `+ - * % || abs lower upper`.
//!
//! This is a recognizer, not DuckDB's parser: it never evaluates or binds SQL,
//! and it does not infer result types. Where the two might disagree on an exotic
//! spelling, it errs toward accepting (DuckDB still has the last word at ATTACH).

use arrow_schema::{DataType, Schema};
use vgi_rpc::{Result, RpcError};

const MAX_SQL: usize = 1024 * 1024;
const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Quoted(String),
    Number(String),
    Str,
    Op(String),
    LParen,
    RParen,
    Comma,
    Dot,
}

fn tokenize(sql: &str) -> std::result::Result<Vec<Token>, String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '-' && chars.get(i + 1) == Some(&'-') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            if i >= chars.len() {
                return Err("unterminated comment".into());
            }
            i += 2;
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let start = i;
            if c == '0' && matches!(chars.get(i + 1), Some('x' | 'X' | 'b' | 'B')) {
                i += 2;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
            } else {
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '_') {
                    i += 1;
                }
                if i < chars.len() && chars[i] == '.' {
                    i += 1;
                    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '_') {
                        i += 1;
                    }
                }
                if i < chars.len() && matches!(chars[i], 'e' | 'E') {
                    let mut j = i + 1;
                    if matches!(chars.get(j), Some('+' | '-')) {
                        j += 1;
                    }
                    if chars.get(j).is_some_and(char::is_ascii_digit) {
                        i = j;
                        while i < chars.len() && chars[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                }
            }
            out.push(Token::Number(chars[start..i].iter().collect()));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            // E'..' / e'..' escape strings are string literals too.
            if (word == "E" || word == "e") && chars.get(i) == Some(&'\'') {
                i = skip_quoted(&chars, i, '\'')?;
                out.push(Token::Str);
            } else {
                out.push(Token::Ident(word));
            }
        } else if c == '"' {
            let start = i + 1;
            i = skip_quoted(&chars, i, '"')?;
            let raw: String = chars[start..i - 1].iter().collect();
            out.push(Token::Quoted(raw.replace("\"\"", "\"")));
        } else if c == '\'' {
            i = skip_quoted(&chars, i, '\'')?;
            out.push(Token::Str);
        } else {
            i += 1;
            match c {
                '(' => out.push(Token::LParen),
                ')' => out.push(Token::RParen),
                ',' => out.push(Token::Comma),
                '.' => out.push(Token::Dot),
                '<' | '>' | '=' | '!' | '|' | ':' | '+' | '-' | '*' | '/' | '%' | '^' | '~'
                | '&' | '#' | '@' | '?' | '[' | ']' | '{' | '}' | ';' => {
                    // Greedy multi-character operators DuckDB recognizes here.
                    let two: String = [c, chars.get(i).copied().unwrap_or(' ')].iter().collect();
                    let op = match two.as_str() {
                        "<=" | ">=" | "<>" | "!=" | "==" | "||" | "::" | "//" | "**" | ":="
                        | "=>" | "->" | "<<" | ">>" | "!~" | "~~" | "^@" => {
                            i += 1;
                            two
                        }
                        _ => c.to_string(),
                    };
                    out.push(Token::Op(op));
                }
                other => return Err(format!("unexpected character '{other}'")),
            }
        }
    }
    Ok(out)
}

fn skip_quoted(chars: &[char], mut i: usize, quote: char) -> std::result::Result<usize, String> {
    i += 1;
    loop {
        match chars.get(i) {
            None => return Err("unterminated quoted text".into()),
            Some(&c) if c == quote => {
                if chars.get(i + 1) == Some(&quote) {
                    i += 2;
                } else {
                    return Ok(i + 1);
                }
            }
            Some('\\') if quote == '\'' => i += 2,
            Some(_) => i += 1,
        }
    }
}

/// The literal types DuckDB assigns, as far as the grammars care.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Literal {
    Null,
    Boolean,
    /// Integer literal that fits BIGINT.
    Integer,
    /// Integer beyond BIGINT (HUGEINT), decimal or float literal.
    OtherNumeric,
    Varchar,
}

#[derive(Debug, Clone, PartialEq)]
enum Expr {
    Literal(Literal),
    /// Column reference: one entry per name part.
    Column(Vec<String>),
    Cast {
        try_cast: bool,
        qualified: bool,
        type_name: String,
        child: Box<Expr>,
    },
    Compare(Box<Expr>, Box<Expr>),
    /// AND / OR.
    Conjunction(Vec<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    Coalesce(Vec<Expr>),
    Function {
        name: String,
        qualified: bool,
        args: Vec<Expr>,
        /// DISTINCT, FILTER, ORDER BY, OVER or named arguments.
        decorated: bool,
    },
    /// Anything outside both grammars (CASE, IN, BETWEEN, LIKE, subqueries,
    /// IS DISTINCT FROM, ...).
    Unsupported(String),
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
}

type P<T> = std::result::Result<T, String>;

fn keyword(token: Option<&Token>, word: &str) -> bool {
    matches!(token, Some(Token::Ident(w)) if w.eq_ignore_ascii_case(word))
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn bump(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn is_op(&self, op: &str) -> bool {
        matches!(self.peek(), Some(Token::Op(o)) if o == op)
    }

    fn eat_keyword(&mut self, word: &str) -> bool {
        if keyword(self.peek(), word) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: Token) -> P<()> {
        if self.peek() == Some(&token) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected {token:?}"))
        }
    }

    fn enter(&mut self) -> P<()> {
        self.depth += 1;
        if self.depth > MAX_DEPTH * 4 {
            return Err("expression nesting is too deep".into());
        }
        Ok(())
    }

    fn expression(&mut self) -> P<Expr> {
        self.enter()?;
        let result = self.or();
        self.depth -= 1;
        result
    }

    fn or(&mut self) -> P<Expr> {
        let mut parts = vec![self.and()?];
        while self.eat_keyword("OR") {
            parts.push(self.and()?);
        }
        Ok(if parts.len() == 1 {
            parts.pop().unwrap()
        } else {
            Expr::Conjunction(parts)
        })
    }

    fn and(&mut self) -> P<Expr> {
        let mut parts = vec![self.not()?];
        while self.eat_keyword("AND") {
            parts.push(self.not()?);
        }
        Ok(if parts.len() == 1 {
            parts.pop().unwrap()
        } else {
            Expr::Conjunction(parts)
        })
    }

    fn not(&mut self) -> P<Expr> {
        if self.eat_keyword("NOT") {
            self.enter()?;
            let inner = self.not();
            self.depth -= 1;
            return Ok(Expr::Not(Box::new(inner?)));
        }
        self.is()
    }

    fn is(&mut self) -> P<Expr> {
        let mut left = self.comparison()?;
        loop {
            if self.eat_keyword("ISNULL") {
                left = Expr::IsNull(Box::new(left));
            } else if self.eat_keyword("NOTNULL") {
                left = Expr::Not(Box::new(Expr::IsNull(Box::new(left))));
            } else if self.eat_keyword("IS") {
                let negated = self.eat_keyword("NOT");
                if self.eat_keyword("NULL") {
                    left = Expr::IsNull(Box::new(left));
                    if negated {
                        left = Expr::Not(Box::new(left));
                    }
                } else {
                    // IS [NOT] TRUE / DISTINCT FROM / ...: outside both grammars.
                    let _ = self.bump();
                    return Ok(Expr::Unsupported("IS predicate".into()));
                }
            } else {
                return Ok(left);
            }
        }
    }

    fn comparison(&mut self) -> P<Expr> {
        let left = self.concat()?;
        const PREDICATES: [&str; 6] = ["IN", "BETWEEN", "LIKE", "ILIKE", "SIMILAR", "GLOB"];
        let predicate = |t: Option<&Token>| PREDICATES.iter().any(|w| keyword(t, w));
        if predicate(self.peek()) || (keyword(self.peek(), "NOT") && predicate(self.peek_at(1))) {
            return Ok(Expr::Unsupported("IN / BETWEEN / LIKE predicate".into()));
        }
        let op = match self.peek() {
            Some(Token::Op(o)) => o.clone(),
            _ => return Ok(left),
        };
        match op.as_str() {
            "=" | "==" | "!=" | "<>" | "<" | ">" | "<=" | ">=" => {
                self.pos += 1;
                let right = self.concat()?;
                Ok(Expr::Compare(Box::new(left), Box::new(right)))
            }
            _ => Ok(left),
        }
    }

    /// `||` and the other generic operators (one precedence level in DuckDB).
    fn concat(&mut self) -> P<Expr> {
        let mut left = self.additive()?;
        loop {
            match self.peek() {
                Some(Token::Op(o)) if o == "||" => {
                    self.pos += 1;
                    let right = self.additive()?;
                    left = function("||", vec![left, right]);
                }
                Some(Token::Op(o))
                    if matches!(
                        o.as_str(),
                        "->" | "<<" | ">>" | "&" | "#" | "~" | "!~" | "~~" | "^@" | "@" | "?"
                    ) =>
                {
                    let o = o.clone();
                    self.pos += 1;
                    let _ = self.additive()?;
                    left = Expr::Unsupported(format!("operator {o}"));
                }
                _ => return Ok(left),
            }
        }
    }

    fn additive(&mut self) -> P<Expr> {
        let mut left = self.multiplicative()?;
        while let Some(Token::Op(o)) = self.peek() {
            let o = o.clone();
            if o != "+" && o != "-" {
                break;
            }
            self.pos += 1;
            let right = self.multiplicative()?;
            left = function(&o, vec![left, right]);
        }
        Ok(left)
    }

    fn multiplicative(&mut self) -> P<Expr> {
        let mut left = self.power()?;
        while let Some(Token::Op(o)) = self.peek() {
            let o = o.clone();
            if !matches!(o.as_str(), "*" | "/" | "%" | "//") {
                break;
            }
            self.pos += 1;
            let right = self.power()?;
            left = function(&o, vec![left, right]);
        }
        Ok(left)
    }

    fn power(&mut self) -> P<Expr> {
        let mut left = self.unary()?;
        while self.is_op("^") || self.is_op("**") {
            self.pos += 1;
            let right = self.unary()?;
            left = function("^", vec![left, right]);
        }
        Ok(left)
    }

    fn unary(&mut self) -> P<Expr> {
        if self.is_op("-") || self.is_op("+") {
            let negative = self.is_op("-");
            self.pos += 1;
            self.enter()?;
            let inner = self.unary();
            self.depth -= 1;
            let inner = inner?;
            // DuckDB folds a signed numeric literal into the constant.
            if let Expr::Literal(Literal::Integer | Literal::OtherNumeric) = inner {
                return Ok(inner);
            }
            return Ok(function(if negative { "-" } else { "+" }, vec![inner]));
        }
        self.postfix()
    }

    fn postfix(&mut self) -> P<Expr> {
        let mut expr = self.primary()?;
        while self.is_op("::") {
            self.pos += 1;
            let (type_name, qualified) = self.type_name()?;
            expr = Expr::Cast {
                try_cast: false,
                qualified,
                type_name,
                child: Box::new(expr),
            };
        }
        if self.is_op("[") {
            return Ok(Expr::Unsupported("subscript".into()));
        }
        Ok(expr)
    }

    /// A type name with optional qualification, `(n[, m])` modifiers and `[]`.
    fn type_name(&mut self) -> P<(String, bool)> {
        let mut parts = Vec::new();
        loop {
            match self.bump() {
                Some(Token::Ident(w)) | Some(Token::Quoted(w)) => parts.push(w),
                _ => return Err("expected a type name".into()),
            }
            if self.peek() == Some(&Token::Dot) {
                self.pos += 1;
                continue;
            }
            break;
        }
        let mut name = parts
            .last()
            .cloned()
            .unwrap_or_default()
            .to_ascii_uppercase();
        // Multi-word spellings.
        if name == "DOUBLE" && self.eat_keyword("PRECISION") {
            name = "DOUBLE PRECISION".into();
        }
        if name == "CHARACTER" && self.eat_keyword("VARYING") {
            name = "CHARACTER VARYING".into();
        }
        if self.peek() == Some(&Token::LParen) {
            self.pos += 1;
            let mut depth = 1;
            while depth > 0 {
                match self.bump() {
                    None => return Err("unterminated type modifier".into()),
                    Some(Token::LParen) => depth += 1,
                    Some(Token::RParen) => depth -= 1,
                    _ => {}
                }
            }
        }
        if self.is_op("[") {
            name.push_str("[]");
            while !self.is_op("]") {
                if self.bump().is_none() {
                    return Err("unterminated array type".into());
                }
            }
            self.pos += 1;
        }
        Ok((name, parts.len() > 1))
    }

    fn primary(&mut self) -> P<Expr> {
        match self.bump() {
            None => Err("unexpected end of expression".into()),
            Some(Token::Number(text)) => Ok(Expr::Literal(number(&text))),
            Some(Token::Str) => Ok(Expr::Literal(Literal::Varchar)),
            Some(Token::LParen) => {
                if keyword(self.peek(), "SELECT") || keyword(self.peek(), "WITH") {
                    return Ok(Expr::Unsupported("subquery".into()));
                }
                let first = self.expression()?;
                if self.peek() == Some(&Token::Comma) {
                    // A row constructor.
                    while self.peek() == Some(&Token::Comma) {
                        self.pos += 1;
                        self.expression()?;
                    }
                    self.expect(Token::RParen)?;
                    return Ok(Expr::Unsupported("row constructor".into()));
                }
                self.expect(Token::RParen)?;
                Ok(first)
            }
            Some(Token::Quoted(name)) => self.name_tail(name),
            Some(Token::Ident(word)) => {
                let upper = word.to_ascii_uppercase();
                match upper.as_str() {
                    "NULL" => return Ok(Expr::Literal(Literal::Null)),
                    "TRUE" | "FALSE" => return Ok(Expr::Literal(Literal::Boolean)),
                    "CASE" | "EXISTS" | "ARRAY" | "INTERVAL" | "SELECT" => {
                        return Ok(Expr::Unsupported(upper));
                    }
                    "CAST" | "TRY_CAST" if self.peek() == Some(&Token::LParen) => {
                        self.pos += 1;
                        let child = self.expression()?;
                        if !self.eat_keyword("AS") {
                            return Err("expected AS in CAST".into());
                        }
                        let (type_name, qualified) = self.type_name()?;
                        self.expect(Token::RParen)?;
                        return Ok(Expr::Cast {
                            try_cast: upper == "TRY_CAST",
                            qualified,
                            type_name,
                            child: Box::new(child),
                        });
                    }
                    _ => {}
                }
                // A typed literal such as DATE '2020-01-01'.
                if self.peek() == Some(&Token::Str) {
                    self.pos += 1;
                    return Ok(Expr::Unsupported("typed literal".into()));
                }
                self.name_tail(word)
            }
            Some(other) => Err(format!("unexpected token {other:?}")),
        }
    }

    /// A (possibly qualified) column reference or function call.
    fn name_tail(&mut self, first: String) -> P<Expr> {
        let mut parts = vec![first];
        while self.peek() == Some(&Token::Dot) {
            self.pos += 1;
            match self.bump() {
                Some(Token::Ident(w)) | Some(Token::Quoted(w)) => parts.push(w),
                Some(Token::Op(o)) if o == "*" => return Ok(Expr::Unsupported("star".into())),
                _ => return Err("expected a name after '.'".into()),
            }
        }
        if self.peek() != Some(&Token::LParen) {
            return Ok(Expr::Column(parts));
        }
        self.pos += 1;
        let mut args = Vec::new();
        let mut decorated = false;
        if self.eat_keyword("DISTINCT") || self.eat_keyword("ALL") {
            decorated = true;
        }
        if self.peek() == Some(&Token::RParen) {
            self.pos += 1;
        } else if self.is_op("*") {
            self.pos += 1;
            self.expect(Token::RParen)?;
            decorated = true;
        } else {
            loop {
                // Named argument: `name := value` or `name => value`.
                if matches!(self.peek(), Some(Token::Ident(_) | Token::Quoted(_)))
                    && matches!(self.peek_at(1), Some(Token::Op(o)) if o == ":=" || o == "=>")
                {
                    self.pos += 2;
                    decorated = true;
                }
                args.push(self.expression()?);
                if self.eat_keyword("ORDER") {
                    decorated = true;
                    while !matches!(self.peek(), Some(Token::RParen) | None) {
                        self.pos += 1;
                    }
                }
                match self.bump() {
                    Some(Token::Comma) => continue,
                    Some(Token::RParen) => break,
                    _ => return Err("expected ',' or ')' in argument list".into()),
                }
            }
        }
        if keyword(self.peek(), "FILTER")
            || keyword(self.peek(), "OVER")
            || keyword(self.peek(), "WITHIN")
        {
            return Ok(Expr::Unsupported("aggregate or window call".into()));
        }
        let name = parts.pop().unwrap_or_default();
        if name.eq_ignore_ascii_case("coalesce") && parts.is_empty() && !decorated {
            return Ok(Expr::Coalesce(args));
        }
        Ok(Expr::Function {
            name,
            qualified: !parts.is_empty(),
            args,
            decorated,
        })
    }
}

fn function(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function {
        name: name.into(),
        qualified: false,
        args,
        decorated: false,
    }
}

fn number(text: &str) -> Literal {
    let digits: String = text.chars().filter(|c| *c != '_').collect();
    let lower = digits.to_ascii_lowercase();
    let integer = if let Some(hex) = lower.strip_prefix("0x") {
        i64::from_str_radix(hex, 16).is_ok()
    } else if let Some(bin) = lower.strip_prefix("0b") {
        i64::from_str_radix(bin, 2).is_ok()
    } else if lower.contains(['.', 'e']) {
        false
    } else {
        // A negated literal folds into the constant, so the magnitude may be
        // one past i64::MAX; both stay BIGINT-or-narrower only within range.
        digits.parse::<i64>().is_ok()
    };
    if integer {
        Literal::Integer
    } else {
        Literal::OtherNumeric
    }
}

/// Parse `sql` as one scalar expression, the way the extension's
/// `"SELECT " + sql` (CHECK) / `"SELECT (" + sql + ")"` (generated) wrapping
/// does: a single trailing `;` is tolerated, anything else after it is not.
fn parse(sql: &str) -> std::result::Result<Expr, String> {
    if sql.is_empty() || sql.len() > MAX_SQL || sql.contains('\0') {
        return Err("invalid SQL size or text".into());
    }
    let mut tokens = tokenize(sql)?;
    while tokens.last() == Some(&Token::Op(";".into())) {
        tokens.pop();
    }
    let mut parser = Parser {
        tokens,
        pos: 0,
        depth: 0,
    };
    let expr = parser.expression()?;
    if parser.pos != parser.tokens.len() {
        return Err("expected exactly one scalar expression without query clauses".into());
    }
    Ok(expr)
}

/// Find the unique top-level column `name` (case-insensitive) in `columns`.
fn column<'a>(
    columns: &'a Schema,
    name: &str,
) -> std::result::Result<&'a arrow_schema::Field, String> {
    let mut found = columns
        .fields()
        .iter()
        .filter(|f| f.name().eq_ignore_ascii_case(name));
    match (found.next(), found.next()) {
        (Some(field), None) => Ok(field),
        (None, _) => Err(format!("references unknown column '{name}'")),
        (Some(_), Some(_)) => Err(format!("column reference '{name}' is ambiguous")),
    }
}

fn is_row_id(field: &arrow_schema::Field) -> bool {
    field.metadata().contains_key("is_row_id")
}

/// The extension's `ParseNumericCheck`: one Boolean comparison of numeric
/// literals and unqualified primitive signed numeric columns of `columns`.
pub(crate) fn check_numeric_comparison(sql: &str, columns: &Schema) -> Result<()> {
    let fail = |message: String| Err(RpcError::value_error(format!("CHECK ({sql}): {message}")));
    let expr = match parse(sql) {
        Ok(expr) => expr,
        Err(e) => return fail(e),
    };
    let Expr::Compare(left, right) = expr else {
        return fail(
            "the DuckDB extension reads only one numeric comparison (=, <>, <, >, <=, >=) \
             between columns and numeric literals"
                .into(),
        );
    };
    for operand in [left, right] {
        match *operand {
            Expr::Literal(Literal::Integer | Literal::OtherNumeric) => {}
            Expr::Literal(_) => return fail("CHECK requires non-NULL numeric literals".into()),
            Expr::Column(parts) if parts.len() == 1 => {
                let field = match column(columns, &parts[0]) {
                    Ok(field) if !is_row_id(field) => field,
                    Ok(_) => return fail("CHECK references the synthetic row-id column".into()),
                    Err(e) => return fail(e),
                };
                if field.metadata().contains_key("ARROW:extension:name") {
                    return fail(format!("column '{}' is extension-typed", field.name()));
                }
                if !matches!(
                    field.data_type(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Float32
                        | DataType::Float64
                ) {
                    return fail(format!(
                        "column '{}' is {}; CHECK requires primitive signed numeric columns \
                         (Int8..Int64, Float32, Float64)",
                        field.name(),
                        field.data_type()
                    ));
                }
            }
            Expr::Column(_) => return fail("CHECK requires unqualified column references".into()),
            _ => {
                return fail(
                    "operands must be numeric literals or column references (no functions, \
                     casts or nested expressions)"
                        .into(),
                )
            }
        }
    }
    Ok(())
}

/// DuckDB spellings of the types a generated expression may cast to
/// (`GeneratedPrimitive`: NULL, BOOLEAN, TINYINT..BIGINT, VARCHAR).
fn generated_cast_type(name: &str) -> bool {
    matches!(
        name,
        "BOOLEAN"
            | "BOOL"
            | "LOGICAL"
            | "TINYINT"
            | "INT1"
            | "SMALLINT"
            | "INT2"
            | "SHORT"
            | "INTEGER"
            | "INT"
            | "INT4"
            | "SIGNED"
            | "INT32"
            | "BIGINT"
            | "INT8"
            | "LONG"
            | "INT64"
            | "VARCHAR"
            | "TEXT"
            | "STRING"
            | "CHAR"
            | "BPCHAR"
            | "NVARCHAR"
            | "CHARACTER VARYING"
            | "NULL"
    )
}

/// Arrow types a generated column, and every column it references, may have.
pub(crate) fn generated_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
    )
}

/// The extension's generated-column grammar (`Parse(sql, true, _, true)`),
/// plus its dependency rule: every referenced column exists in `columns` and
/// has a [`generated_type`].
pub(crate) fn check_generated(sql: &str, columns: &Schema) -> Result<()> {
    let fail = |message: String| {
        Err(RpcError::value_error(format!(
            "generated expression ({sql}): {message}"
        )))
    };
    let expr = match parse(sql) {
        Ok(expr) => expr,
        Err(e) => return fail(e),
    };
    let mut nodes = 0;
    if let Err(e) = generated_node(&expr, columns, 0, &mut nodes) {
        return fail(e);
    }
    Ok(())
}

fn generated_node(expr: &Expr, columns: &Schema, depth: usize, nodes: &mut usize) -> P<()> {
    *nodes += 1;
    if depth > MAX_DEPTH || *nodes > MAX_NODES {
        return Err("scalar expression is too complex".into());
    }
    let children: Vec<&Expr> = match expr {
        Expr::Literal(Literal::OtherNumeric) => {
            return Err(
                "unsupported scalar literal type (only NULL, BOOLEAN, BIGINT-range \
                        integers and strings)"
                    .into(),
            )
        }
        Expr::Literal(_) => Vec::new(),
        Expr::Column(parts) => {
            if parts.len() != 1 {
                return Err("column references must be unqualified".into());
            }
            let field = column(columns, &parts[0])?;
            if !generated_type(field.data_type())
                || field.metadata().contains_key("ARROW:extension:name")
            {
                return Err(format!(
                    "unsupported generated operand type {} of column '{}'",
                    field.data_type(),
                    field.name()
                ));
            }
            Vec::new()
        }
        Expr::Cast {
            try_cast,
            qualified,
            type_name,
            child,
        } => {
            if *try_cast || *qualified || !generated_cast_type(type_name) {
                return Err(format!("unsupported scalar cast to {type_name}"));
            }
            vec![child]
        }
        Expr::Compare(a, b) => vec![a, b],
        Expr::Conjunction(parts) | Expr::Coalesce(parts) => parts.iter().collect(),
        Expr::Not(a) | Expr::IsNull(a) => vec![a],
        Expr::Function {
            name,
            qualified,
            args,
            decorated,
        } => {
            let lower = name.to_ascii_lowercase();
            let allowed = matches!(
                lower.as_str(),
                "+" | "-" | "*" | "%" | "abs" | "lower" | "upper"
            ) || name == "||";
            if *qualified || *decorated || !allowed {
                return Err(format!("unsupported scalar function '{name}'"));
            }
            args.iter().collect()
        }
        Expr::Unsupported(what) => {
            return Err(format!(
                "{what} is outside the supported deterministic scalar grammar"
            ))
        }
    };
    for child in children {
        generated_node(child, columns, depth + 1, nodes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("Price", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("d", DataType::Decimal128(10, 2), true),
            Field::new("rowid", DataType::Int64, false).with_metadata(
                std::collections::HashMap::from([("is_row_id".to_string(), "true".to_string())]),
            ),
        ])
    }

    #[test]
    fn check_accepts_numeric_comparisons() {
        for sql in [
            "a >= 0",
            "a > -5",
            "0 < a",
            "price <= 1.5e3",
            "(a <> 3)",
            "\"a\" = 1;",
            "a != price",
            "a == 1_000",
            "(a) > (0)",
        ] {
            check_numeric_comparison(sql, &schema()).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[test]
    fn check_rejects_everything_else() {
        for sql in [
            "",
            "a",
            "a > 0 AND a < 9",
            "NOT a > 0",
            "a + 1 > 0",
            "abs(a) > 0",
            "a > '1'",
            "a > NULL",
            "flag = TRUE",
            "name > 0",
            "d > 0",
            "t.a > 0",
            "rowid > 0",
            "missing > 0",
            "a::INT > 0",
            "a BETWEEN 1 AND 2",
            "a IN (1, 2)",
            "a IS NULL",
            "a > 0 a",
            "a > 0; SELECT 1",
        ] {
            assert!(
                check_numeric_comparison(sql, &schema()).is_err(),
                "accepted {sql}"
            );
        }
    }

    #[test]
    fn generated_grammar() {
        for sql in [
            "a * 2",
            "-a",
            "CAST(\"a\" AS BIGINT)",
            "a::INTEGER + 1",
            "lower(name) || 'x'",
            "UPPER(name)",
            "coalesce(a, 0)",
            "a IS NOT NULL AND flag",
            "NOT flag OR a > 1",
            "abs(a) % 3",
            "NULL",
            "CAST(a AS VARCHAR(10))",
        ] {
            check_generated(sql, &schema()).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        for sql in [
            "a / 2",
            "a * 1.5",
            "price + 1",
            "TRY_CAST(a AS BIGINT)",
            "CAST(a AS DOUBLE)",
            "CASE WHEN flag THEN 1 ELSE 0 END",
            "concat(name, 'x')",
            "main.lower(name)",
            "a IN (1, 2)",
            "missing + 1",
            "x.a + 1",
            "(SELECT 1)",
            "lower(name) AS x",
            "a IS DISTINCT FROM 1",
            "a ^ 2",
        ] {
            assert!(check_generated(sql, &schema()).is_err(), "accepted {sql}");
        }
    }
}
