use std::fmt::Display;
use std::iter::Peekable;
use std::str::Chars;

pub type Result<T> = ::std::result::Result<T, ExprError>;

#[derive(Debug)]
pub enum ExprError {
    Parse(String),
}
impl std::error::Error for ExprError {}
impl Display for ExprError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Parse(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Token {
    Number(i32),
    Plus,
    Minus,
    Multiply,
    Divide,
    Power,
    LeftParen,
    RightParen,
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Token::Number(n) => n.to_string(),
                Token::Plus => "+".to_string(),
                Token::Minus => "-".to_string(),
                Token::Multiply => "*".to_string(),
                Token::Divide => "/".to_string(),
                Token::Power => "^".to_string(),
                Token::LeftParen => "(".to_string(),
                Token::RightParen => ")".to_string(),
            }
        )
    }
}

impl Token {
    fn is_operator(&self) -> bool {
        match self {
            Token::Plus | Token::Minus | Token::Multiply | Token::Divide | Token::Power => true,
            _ => false,
        }
    }

    fn precedence(&self) -> u8 {
        match self {
            Token::Plus | Token::Minus => 1,
            Token::Multiply | Token::Divide => 2,
            Token::Power => 3,
            _ => 0,
        }
    }

    fn assoc(&self) -> u8 {
        match self {
            Token::Power => ASSOC_RIGHT,
            _ => ASSOC_LEFT,
        }
    }

    fn compute(&self, l: i32, r: i32) -> Option<i32> {
        match self {
            Token::Plus => Some(l + r),
            Token::Minus => Some(l - r),
            Token::Multiply => Some(l * r),
            Token::Divide => Some(l / r),
            Token::Power => Some(l.pow(r as u32)),
            _ => None,
        }
    }
}
const ASSOC_LEFT: u8 = 0;
const ASSOC_RIGHT: u8 = 1;

struct Tokenizer<'a> {
    tokens: Peekable<Chars<'a>>,
}

impl<'a> Tokenizer<'a> {
    fn new(expr: &'a str) -> Self {
        Self {
            tokens: expr.chars().peekable(),
        }
    }
    fn consume_whitespace(&mut self) {
        while let Some(&c) = self.tokens.peek() {
            if c.is_whitespace() {
                self.tokens.next();
            } else {
                break;
            }
        }
    }
    fn scan_number(&mut self) -> Option<Token> {
        let mut num = String::new();
        while let Some(&c) = self.tokens.peek() {
            if c.is_numeric() {
                num.push(c);
                self.tokens.next();
            } else {
                break;
            }
        }
        match num.parse() {
            Ok(n) => Some(Token::Number(n)),
            Err(_) => None,
        }
    }
    fn scan_operator(&mut self) -> Option<Token> {
        match self.tokens.next() {
            Some('+') => Some(Token::Plus),
            Some('-') => Some(Token::Minus),
            Some('*') => Some(Token::Multiply),
            Some('/') => Some(Token::Divide),
            Some('^') => Some(Token::Power),
            Some('(') => Some(Token::LeftParen),
            Some(')') => Some(Token::RightParen),
            _ => None,
        }
    }
}

impl<'a> Iterator for Tokenizer<'a> {
    type Item = Token;
    fn next(&mut self) -> Option<Self::Item> {
        self.consume_whitespace();
        match self.tokens.peek() {
            Some(c) if c.is_numeric() => self.scan_number(),
            Some(_) => self.scan_operator(),
            None => None,
        }
    }
}
struct Expr<'a> {
    iter: Peekable<Tokenizer<'a>>,
}

impl<'a> Expr<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            iter: Tokenizer::new(src).peekable(),
        }
    }
    fn eval(&mut self) -> Result<i32> {
        let result = self.compute_expr(1)?;
        if self.iter.peek().is_some() {
            return Err(ExprError::Parse("Unexpected end of expr".into()));
        }
        Ok(result)
    }

    fn compute_expr(&mut self, min_prec: u8) -> Result<i32> {
        let mut atom_lhs = self.compute_atom()?;
        loop {
            let cur_token = self.iter.peek();
            if cur_token.is_none() {
                break;
            }
            let token = *cur_token.unwrap();
            if !token.is_operator() || token.precedence() < min_prec {
                break;
            }
            // 若优先级更高，就继续，算出右边的表达式，不然直接先返回
            // 比如92 + 5，是先返回5
            // 97+5*27，5遇到*，*优先级大于+，就先算完。
            let mut next_prec = token.precedence();
            if token.assoc() == ASSOC_LEFT {
                next_prec += 1;
            }
            self.iter.next(); // 上面的cur_token，要move，不然这里被借2次

            let atom_rhs = self.compute_expr(next_prec)?;
            match token.compute(atom_lhs, atom_rhs) {
                Some(res) => atom_lhs = res,
                None => return Err(ExprError::Parse("Unexpected expr".into())),
            }
            println!("token {:?} lhs {:?}", &token, &atom_lhs);
        }
        Ok(atom_lhs)
    }

    fn compute_atom(&mut self) -> Result<i32> {
        match self.iter.peek() {
            Some(Token::Number(n)) => {
                let val = *n;
                self.iter.next();
                Ok(val)
            }
            Some(Token::LeftParen) => {
                self.iter.next();
                let result = self.compute_expr(1)?;
                match self.iter.next() {
                    Some(Token::RightParen) => (),
                    _ => return Err(ExprError::Parse("Unexpected character".into())),
                }
                Ok(result)
            }
            _ => Err(ExprError::Parse(
                "Expecting a number or left parenthesis".into(),
            )),
        }
    }
}

// 调度场算法（Shunting Yard Algorithm）
// 将中缀表达式转换为后缀表达式，然后求值
fn shunting_yard_evaluate(expr: &str) -> Result<i32> {
    let tokens = tokenize_simple(expr)?;
    let postfix = infix_to_postfix(tokens)?;
    evaluate_postfix(postfix)
}

// 简单的分词函数
fn tokenize_simple(expr: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current_number = String::new();
    
    for ch in expr.chars() {
        if ch.is_whitespace() {
            continue;
        }
        
        if ch.is_numeric() {
            current_number.push(ch);
        } else {
            if !current_number.is_empty() {
                tokens.push(current_number.clone());
                current_number.clear();
            }
            tokens.push(ch.to_string());
        }
    }
    
    if !current_number.is_empty() {
        tokens.push(current_number);
    }
    
    Ok(tokens)
}

// 中缀转后缀（调度场算法）
fn infix_to_postfix(tokens: Vec<String>) -> Result<Vec<String>> {
    let mut output = Vec::new();
    let mut operator_stack = Vec::new();
    
    for token in tokens {
        if token.chars().all(|c| c.is_numeric()) {
            // 数字直接输出
            output.push(token);
        } else if token == "(" {
            // 左括号压入栈
            operator_stack.push(token);
        } else if token == ")" {
            // 右括号：弹出操作符直到遇到左括号
            while let Some(op) = operator_stack.pop() {
                if op == "(" {
                    break;
                }
                output.push(op);
            }
        } else if is_operator(&token) {
            // 操作符：根据优先级和结合性决定是否弹出栈中操作符
            while let Some(top) = operator_stack.last() {
                if top == "(" {
                    break;
                }
                
                let current_prec = get_precedence(&token);
                let top_prec = get_precedence(top);
                
                // 左结合：当前优先级 <= 栈顶优先级时弹出
                // 右结合：当前优先级 < 栈顶优先级时弹出
                let should_pop = if is_right_associative(&token) {
                    current_prec < top_prec
                } else {
                    current_prec <= top_prec
                };
                
                if !should_pop {
                    break;
                }
                output.push(operator_stack.pop().unwrap());
            }
            operator_stack.push(token);
        }
    }
    
    // 弹出剩余操作符
    while let Some(op) = operator_stack.pop() {
        if op == "(" || op == ")" {
            return Err(ExprError::Parse("Mismatched parentheses".into()));
        }
        output.push(op);
    }
    
    Ok(output)
}

// 求值后缀表达式
fn evaluate_postfix(tokens: Vec<String>) -> Result<i32> {
    let mut stack = Vec::new();
    
    for token in tokens {
        if token.chars().all(|c| c.is_numeric()) {
            let num = token.parse::<i32>().map_err(|_| ExprError::Parse("Invalid number".into()))?;
            stack.push(num);
        } else if is_operator(&token) {
            if stack.len() < 2 {
                return Err(ExprError::Parse("Invalid expression".into()));
            }
            let b = stack.pop().unwrap();
            let a = stack.pop().unwrap();
            let result = match token.as_str() {
                "+" => a + b,
                "-" => a - b,
                "*" => a * b,
                "/" => {
                    if b == 0 {
                        return Err(ExprError::Parse("Division by zero".into()));
                    }
                    a / b
                },
                "^" => a.pow(b as u32),
                _ => return Err(ExprError::Parse("Unknown operator".into())),
            };
            stack.push(result);
        }
    }
    
    if stack.len() == 1 {
        Ok(stack[0])
    } else {
        Err(ExprError::Parse("Invalid expression".into()))
    }
}

// 直接求值的双栈算法
fn direct_evaluation(expr: &str) -> Result<i32> {
    let mut num_stack: Vec<i32> = Vec::new();
    let mut op_stack: Vec<char> = Vec::new();
    
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;
    
    while i < chars.len() {
        let ch = chars[i];
        
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        
        if ch.is_numeric() {
            // 解析数字
            let mut num = 0;
            while i < chars.len() && chars[i].is_numeric() {
                num = num * 10 + (chars[i] as i32 - '0' as i32);
                i += 1;
            }
            num_stack.push(num);
        } else if ch == '(' {
            // 左括号直接压入操作符栈
            op_stack.push(ch);
            i += 1;
        } else if ch == ')' {
            // 右括号：计算直到遇到左括号
            while let Some(&top_op) = op_stack.last() {
                if top_op == '(' {
                    op_stack.pop(); // 弹出左括号
                    break;
                }
                apply_operator(&mut num_stack, &mut op_stack)?;
            }
            i += 1;
        } else if is_operator_char(ch) {
            // 操作符：根据优先级和结合性决定是否先计算栈中的操作符
            while let Some(&top_op) = op_stack.last() {
                if top_op == '(' {
                    break;
                }
                
                let current_prec = get_precedence_char(ch);
                let top_prec = get_precedence_char(top_op);
                
                // 左结合：当前优先级 <= 栈顶优先级时先计算栈顶
                // 右结合：当前优先级 < 栈顶优先级时先计算栈顶
                let should_calculate = if is_right_associative_char(ch) {
                    current_prec < top_prec
                } else {
                    current_prec <= top_prec
                };
                
                if !should_calculate {
                    break;
                }
                apply_operator(&mut num_stack, &mut op_stack)?;
            }
            op_stack.push(ch);
            i += 1;
        } else {
            return Err(ExprError::Parse(format!("Unknown character: {}", ch)));
        }
    }
    
    // 处理剩余的操作符
    while !op_stack.is_empty() {
        apply_operator(&mut num_stack, &mut op_stack)?;
    }
    
    if num_stack.len() == 1 {
        Ok(num_stack[0])
    } else {
        Err(ExprError::Parse("Invalid expression".into()))
    }
}

// 辅助函数
fn is_operator(token: &str) -> bool {
    matches!(token, "+" | "-" | "*" | "/" | "^")
}

fn is_operator_char(ch: char) -> bool {
    matches!(ch, '+' | '-' | '*' | '/' | '^')
}

fn get_precedence(op: &str) -> i32 {
    match op {
        "+" | "-" => 1,
        "*" | "/" => 2,
        "^" => 3,
        _ => 0,
    }
}

fn get_precedence_char(op: char) -> i32 {
    match op {
        '+' | '-' => 1,
        '*' | '/' => 2,
        '^' => 3,
        _ => 0,
    }
}

fn is_right_associative(op: &str) -> bool {
    op == "^"
}

fn is_right_associative_char(op: char) -> bool {
    op == '^'
}

fn apply_operator(num_stack: &mut Vec<i32>, op_stack: &mut Vec<char>) -> Result<()> {
    if num_stack.len() < 2 || op_stack.is_empty() {
        return Err(ExprError::Parse("Invalid expression".into()));
    }
    
    let b = num_stack.pop().unwrap();
    let a = num_stack.pop().unwrap();
    let op = op_stack.pop().unwrap();
    
    let result = match op {
        '+' => a + b,
        '-' => a - b,
        '*' => a * b,
        '/' => {
            if b == 0 {
                return Err(ExprError::Parse("Division by zero".into()));
            }
            a / b
        },
        '^' => a.pow(b as u32),
        _ => return Err(ExprError::Parse("Unknown operator".into())),
    };
    
    num_stack.push(result);
    Ok(())
}

fn main() {
    let test_cases = vec![
        "92 + 5 + 5 * 27 - (92 - 12) / 4 + 26",
        "2 + 3 * 4",
        "(2 + 3) * 4", 
        "2 * (3 + 4)",
        "((2 + 3) * 4 - 1) / 3",
        "2^3^2", // 右结合：2^(3^2) = 2^9 = 512
        "10 - 5 - 2", // 左结合：(10 - 5) - 2 = 3
        "8 / 4 / 2", // 左结合：(8 / 4) / 2 = 1
        "2^3^2", // 右结合：2^(3^2) = 512
        "4^3^2", // 右结合：4^(3^2) = 4^9 = 262144
    ];
    
    println!("=== 测试表达式求值器 ===\n");
    
    for expr in test_cases {
        println!("表达式: {}", expr);
        
        // 使用原有的递归下降解析器
        let mut parser = Expr::new(expr);
        match parser.eval() {
            Ok(result) => println!("递归下降解析器结果: {}", result),
            Err(e) => println!("递归下降解析器错误: {}", e),
        }
        
        // 使用调度场算法
        match shunting_yard_evaluate(expr) {
            Ok(result) => println!("调度场算法结果: {}", result),
            Err(e) => println!("调度场算法错误: {}", e),
        }
        
        // 使用直接求值双栈算法
        match direct_evaluation(expr) {
            Ok(result) => println!("直接求值算法结果: {}", result),
            Err(e) => println!("直接求值算法错误: {}", e),
        }
        
        println!("---");
    }
}
