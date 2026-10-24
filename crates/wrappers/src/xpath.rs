//! XPath 1.0 over the arena DOM of `xml_dom`: lexer, parser and evaluator.
//!
//! Everything in the 1.0 core is supported except the `namespace` axis (always empty), `id()`,
//! `lang()` and processing instructions (the DOM drops them). Name tests compare qualified names
//! as written in the document, so `atom:entry` matches an element with that prefix; use
//! `*[local-name()='entry']` to ignore prefixes. Evaluation is bounded by a step budget and the
//! invocation deadline.

use std::{cell::Cell, collections::HashMap};

use crate::{
    xml_dom::{Dom, Kind, NodeRef},
    Deadline,
};

const MAX_TICKS: u64 = 50_000_000;
const MAX_EXPR_DEPTH: usize = 64;

#[derive(Clone, Debug)]
pub enum Val {
    Nodes(Vec<NodeRef>),
    Str(String),
    Num(f64),
    Bool(bool),
}

// ---------------------------------------------------------------- lexer

#[derive(Clone, PartialEq, Debug)]
enum Tok {
    Slash,
    DSlash,
    Dot,
    DDot,
    At,
    Comma,
    LParen,
    RParen,
    LBrack,
    RBrack,
    Pipe,
    Plus,
    Minus,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    Star,
    Dollar,
    ColonColon,
    Num(f64),
    Lit(String),
    Name(String),
    And,
    Or,
    Mod,
    Div,
    Mul,
}

fn ncname_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn ncname_char(c: char) -> bool {
    ncname_start(c) || c.is_numeric() || matches!(c, '-' | '.' | '\u{b7}')
}

/// After these tokens (or at the start) `*` and and/or/mod/div are names, not operators.
fn operand_expected(prev: Option<&Tok>) -> bool {
    use Tok::*;
    matches!(
        prev,
        None | Some(
            At | ColonColon
                | LParen
                | LBrack
                | Comma
                | And
                | Or
                | Mod
                | Div
                | Mul
                | Slash
                | DSlash
                | Pipe
                | Plus
                | Minus
                | Eq
                | Neq
                | Lt
                | Le
                | Gt
                | Ge
        )
    )
}

fn lex(src: &str) -> Result<Vec<Tok>, String> {
    let cs: Vec<char> = src.chars().collect();
    let mut out: Vec<Tok> = Vec::new();
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        let next = cs.get(i + 1).copied();
        match c {
            c if c.is_whitespace() => {
                i += 1;
                continue;
            }
            '/' if next == Some('/') => {
                out.push(Tok::DSlash);
                i += 2;
                continue;
            }
            '/' => out.push(Tok::Slash),
            '.' if next == Some('.') => {
                out.push(Tok::DDot);
                i += 2;
                continue;
            }
            '.' if next.is_some_and(|n| n.is_ascii_digit()) => {
                let (t, n) = lex_number(&cs, i)?;
                out.push(t);
                i = n;
                continue;
            }
            '.' => out.push(Tok::Dot),
            '@' => out.push(Tok::At),
            ',' => out.push(Tok::Comma),
            '(' => out.push(Tok::LParen),
            ')' => out.push(Tok::RParen),
            '[' => out.push(Tok::LBrack),
            ']' => out.push(Tok::RBrack),
            '|' => out.push(Tok::Pipe),
            '+' => out.push(Tok::Plus),
            '-' => out.push(Tok::Minus),
            '=' => out.push(Tok::Eq),
            '$' => out.push(Tok::Dollar),
            '!' if next == Some('=') => {
                out.push(Tok::Neq);
                i += 2;
                continue;
            }
            '<' if next == Some('=') => {
                out.push(Tok::Le);
                i += 2;
                continue;
            }
            '<' => out.push(Tok::Lt),
            '>' if next == Some('=') => {
                out.push(Tok::Ge);
                i += 2;
                continue;
            }
            '>' => out.push(Tok::Gt),
            ':' if next == Some(':') => {
                out.push(Tok::ColonColon);
                i += 2;
                continue;
            }
            '*' => {
                out.push(if operand_expected(out.last()) {
                    Tok::Star
                } else {
                    Tok::Mul
                });
            }
            '"' | '\'' => {
                let end = cs[i + 1..]
                    .iter()
                    .position(|x| *x == c)
                    .ok_or("unterminated string literal")?;
                out.push(Tok::Lit(cs[i + 1..i + 1 + end].iter().collect()));
                i += end + 2;
                continue;
            }
            c if c.is_ascii_digit() => {
                let (t, n) = lex_number(&cs, i)?;
                out.push(t);
                i = n;
                continue;
            }
            c if ncname_start(c) => {
                let mut j = i;
                while j < cs.len() && ncname_char(cs[j]) {
                    j += 1;
                }
                // prefix:local or prefix:*  (but not the "::" of an axis)
                if cs.get(j) == Some(&':') && cs.get(j + 1) != Some(&':') {
                    if cs.get(j + 1) == Some(&'*') {
                        j += 2;
                    } else if cs.get(j + 1).is_some_and(|c| ncname_start(*c)) {
                        j += 1;
                        while j < cs.len() && ncname_char(cs[j]) {
                            j += 1;
                        }
                    }
                }
                let name: String = cs[i..j].iter().collect();
                let tok = if operand_expected(out.last()) {
                    Tok::Name(name)
                } else {
                    match name.as_str() {
                        "and" => Tok::And,
                        "or" => Tok::Or,
                        "mod" => Tok::Mod,
                        "div" => Tok::Div,
                        _ => Tok::Name(name),
                    }
                };
                out.push(tok);
                i = j;
                continue;
            }
            c => return Err(format!("unexpected character {c:?}")),
        }
        i += 1;
    }
    Ok(out)
}

fn lex_number(cs: &[char], start: usize) -> Result<(Tok, usize), String> {
    let mut j = start;
    while j < cs.len() && cs[j].is_ascii_digit() {
        j += 1;
    }
    if cs.get(j) == Some(&'.') {
        j += 1;
        while j < cs.len() && cs[j].is_ascii_digit() {
            j += 1;
        }
    }
    let s: String = cs[start..j].iter().collect();
    let n = s.parse::<f64>().map_err(|_| format!("bad number {s:?}"))?;
    Ok((Tok::Num(n), j))
}

// --------------------------------------------------------------- parser

#[derive(Clone, Copy, PartialEq, Debug)]
enum Axis {
    Child,
    Descendant,
    Parent,
    Ancestor,
    FollowingSibling,
    PrecedingSibling,
    Following,
    Preceding,
    Attribute,
    Namespace,
    Itself,
    DescendantOrSelf,
    AncestorOrSelf,
}

#[derive(Clone, Debug)]
enum Test {
    Name(String),
    Star,
    PrefixStar(String),
    Node,
    Text,
    Comment,
    Pi,
}

#[derive(Clone, Debug)]
pub struct Step {
    axis: Axis,
    test: Test,
    preds: Vec<Expr>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Op {
    Or,
    And,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Debug)]
pub enum Expr {
    Num(f64),
    Str(String),
    Var(String),
    Neg(Box<Expr>),
    Bin(Op, Box<Expr>, Box<Expr>),
    Union(Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
    Filter(Box<Expr>, Vec<Expr>),
    Path {
        start: Option<Box<Expr>>,
        absolute: bool,
        steps: Vec<Step>,
    },
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
    depth: usize,
}

pub fn compile(src: &str) -> Result<Expr, String> {
    let mut p = Parser {
        toks: lex(src)?,
        pos: 0,
        depth: 0,
    };
    let e = p.or_expr()?;
    if let Some(t) = p.peek() {
        return Err(format!("unexpected {t:?}"));
    }
    Ok(e)
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn peek_at(&self, n: usize) -> Option<&Tok> {
        self.toks.get(self.pos + n)
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, t: Tok) -> Result<(), String> {
        if self.eat(&t) {
            Ok(())
        } else {
            Err(format!("expected {t:?}, found {:?}", self.peek()))
        }
    }

    fn or_expr(&mut self) -> Result<Expr, String> {
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err("expression nested too deeply".into());
        }
        let mut l = self.and_expr()?;
        while self.eat(&Tok::Or) {
            l = Expr::Bin(Op::Or, Box::new(l), Box::new(self.and_expr()?));
        }
        self.depth -= 1;
        Ok(l)
    }

    fn and_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.eq_expr()?;
        while self.eat(&Tok::And) {
            l = Expr::Bin(Op::And, Box::new(l), Box::new(self.eq_expr()?));
        }
        Ok(l)
    }

    fn eq_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.rel_expr()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Eq) => Op::Eq,
                Some(Tok::Neq) => Op::Neq,
                _ => return Ok(l),
            };
            self.pos += 1;
            l = Expr::Bin(op, Box::new(l), Box::new(self.rel_expr()?));
        }
    }

    fn rel_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.add_expr()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Lt) => Op::Lt,
                Some(Tok::Le) => Op::Le,
                Some(Tok::Gt) => Op::Gt,
                Some(Tok::Ge) => Op::Ge,
                _ => return Ok(l),
            };
            self.pos += 1;
            l = Expr::Bin(op, Box::new(l), Box::new(self.add_expr()?));
        }
    }

    fn add_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.mul_expr()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Plus) => Op::Add,
                Some(Tok::Minus) => Op::Sub,
                _ => return Ok(l),
            };
            self.pos += 1;
            l = Expr::Bin(op, Box::new(l), Box::new(self.mul_expr()?));
        }
    }

    fn mul_expr(&mut self) -> Result<Expr, String> {
        let mut l = self.unary()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Mul) => Op::Mul,
                Some(Tok::Div) => Op::Div,
                Some(Tok::Mod) => Op::Mod,
                _ => return Ok(l),
            };
            self.pos += 1;
            l = Expr::Bin(op, Box::new(l), Box::new(self.unary()?));
        }
    }

    fn unary(&mut self) -> Result<Expr, String> {
        if self.eat(&Tok::Minus) {
            return Ok(Expr::Neg(Box::new(self.unary()?)));
        }
        let mut l = self.path()?;
        while self.eat(&Tok::Pipe) {
            l = Expr::Union(Box::new(l), Box::new(self.path()?));
        }
        Ok(l)
    }

    fn is_node_type(name: &str) -> bool {
        matches!(name, "node" | "text" | "comment" | "processing-instruction")
    }

    fn starts_primary(&self) -> bool {
        match self.peek() {
            Some(Tok::Num(_) | Tok::Lit(_) | Tok::Dollar | Tok::LParen) => true,
            Some(Tok::Name(n)) => self.peek_at(1) == Some(&Tok::LParen) && !Self::is_node_type(n),
            _ => false,
        }
    }

    fn starts_step(&self) -> bool {
        matches!(
            self.peek(),
            Some(Tok::Name(_) | Tok::Star | Tok::At | Tok::Dot | Tok::DDot)
        )
    }

    fn path(&mut self) -> Result<Expr, String> {
        if self.eat(&Tok::Slash) {
            let steps = if self.starts_step() {
                self.relative_steps()?
            } else {
                Vec::new()
            };
            return Ok(Expr::Path {
                start: None,
                absolute: true,
                steps,
            });
        }
        if self.eat(&Tok::DSlash) {
            let mut steps = vec![desc_or_self()];
            steps.extend(self.relative_steps()?);
            return Ok(Expr::Path {
                start: None,
                absolute: true,
                steps,
            });
        }
        if self.starts_primary() {
            let mut e = self.primary()?;
            let mut preds = Vec::new();
            while self.peek() == Some(&Tok::LBrack) {
                preds.push(self.predicate()?);
            }
            if !preds.is_empty() {
                e = Expr::Filter(Box::new(e), preds);
            }
            let mut steps = Vec::new();
            if self.eat(&Tok::Slash) {
                steps = self.relative_steps()?;
            } else if self.eat(&Tok::DSlash) {
                steps.push(desc_or_self());
                steps.extend(self.relative_steps()?);
            }
            if steps.is_empty() {
                return Ok(e);
            }
            return Ok(Expr::Path {
                start: Some(Box::new(e)),
                absolute: false,
                steps,
            });
        }
        Ok(Expr::Path {
            start: None,
            absolute: false,
            steps: self.relative_steps()?,
        })
    }

    fn relative_steps(&mut self) -> Result<Vec<Step>, String> {
        let mut steps = vec![self.step()?];
        loop {
            if self.eat(&Tok::Slash) {
            } else if self.eat(&Tok::DSlash) {
                steps.push(desc_or_self());
            } else {
                return Ok(steps);
            }
            steps.push(self.step()?);
        }
    }

    fn step(&mut self) -> Result<Step, String> {
        if self.eat(&Tok::Dot) {
            return Ok(Step {
                axis: Axis::Itself,
                test: Test::Node,
                preds: Vec::new(),
            });
        }
        if self.eat(&Tok::DDot) {
            return Ok(Step {
                axis: Axis::Parent,
                test: Test::Node,
                preds: Vec::new(),
            });
        }
        let mut axis = Axis::Child;
        if self.eat(&Tok::At) {
            axis = Axis::Attribute;
        } else if let (Some(Tok::Name(n)), Some(Tok::ColonColon)) = (self.peek(), self.peek_at(1)) {
            axis = match n.as_str() {
                "child" => Axis::Child,
                "descendant" => Axis::Descendant,
                "parent" => Axis::Parent,
                "ancestor" => Axis::Ancestor,
                "following-sibling" => Axis::FollowingSibling,
                "preceding-sibling" => Axis::PrecedingSibling,
                "following" => Axis::Following,
                "preceding" => Axis::Preceding,
                "attribute" => Axis::Attribute,
                "namespace" => Axis::Namespace,
                "self" => Axis::Itself,
                "descendant-or-self" => Axis::DescendantOrSelf,
                "ancestor-or-self" => Axis::AncestorOrSelf,
                other => return Err(format!("unknown axis {other:?}")),
            };
            self.pos += 2;
        }
        let test = match self.peek().cloned() {
            Some(Tok::Star) => {
                self.pos += 1;
                Test::Star
            }
            Some(Tok::Name(n)) => {
                self.pos += 1;
                if Self::is_node_type(&n) && self.peek() == Some(&Tok::LParen) {
                    self.pos += 1;
                    if let Some(Tok::Lit(_)) = self.peek() {
                        self.pos += 1;
                    }
                    self.expect(Tok::RParen)?;
                    match n.as_str() {
                        "node" => Test::Node,
                        "text" => Test::Text,
                        "comment" => Test::Comment,
                        _ => Test::Pi,
                    }
                } else if let Some(p) = n.strip_suffix(":*") {
                    Test::PrefixStar(p.to_string())
                } else {
                    Test::Name(n)
                }
            }
            t => return Err(format!("expected a node test, found {t:?}")),
        };
        let mut preds = Vec::new();
        while self.peek() == Some(&Tok::LBrack) {
            preds.push(self.predicate()?);
        }
        Ok(Step { axis, test, preds })
    }

    fn predicate(&mut self) -> Result<Expr, String> {
        self.expect(Tok::LBrack)?;
        let e = self.or_expr()?;
        self.expect(Tok::RBrack)?;
        Ok(e)
    }

    fn primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Tok::Num(n)) => {
                self.pos += 1;
                Ok(Expr::Num(n))
            }
            Some(Tok::Lit(s)) => {
                self.pos += 1;
                Ok(Expr::Str(s))
            }
            Some(Tok::Dollar) => {
                self.pos += 1;
                match self.peek().cloned() {
                    Some(Tok::Name(n)) => {
                        self.pos += 1;
                        Ok(Expr::Var(n))
                    }
                    _ => Err("expected a variable name after $".into()),
                }
            }
            Some(Tok::LParen) => {
                self.pos += 1;
                let e = self.or_expr()?;
                self.expect(Tok::RParen)?;
                Ok(e)
            }
            Some(Tok::Name(n)) => {
                self.pos += 2; // name and "("
                let mut args = Vec::new();
                if !self.eat(&Tok::RParen) {
                    loop {
                        args.push(self.or_expr()?);
                        if self.eat(&Tok::RParen) {
                            break;
                        }
                        self.expect(Tok::Comma)?;
                    }
                }
                Ok(Expr::Call(n, args))
            }
            t => Err(format!("unexpected {t:?}")),
        }
    }
}

fn desc_or_self() -> Step {
    Step {
        axis: Axis::DescendantOrSelf,
        test: Test::Node,
        preds: Vec::new(),
    }
}

// ------------------------------------------------------------ evaluator

pub type Vars = HashMap<String, Val>;

#[derive(Clone, Copy)]
struct Ctx {
    node: NodeRef,
    pos: usize,
    size: usize,
}

struct Eval<'a> {
    dom: &'a Dom,
    vars: &'a Vars,
    ticks: Cell<u64>,
    deadline: &'a Deadline,
}

pub fn eval(
    dom: &Dom,
    expr: &Expr,
    node: NodeRef,
    vars: &Vars,
    deadline: &Deadline,
) -> Result<Val, String> {
    let ev = Eval {
        dom,
        vars,
        ticks: Cell::new(0),
        deadline,
    };
    ev.eval(
        expr,
        Ctx {
            node,
            pos: 1,
            size: 1,
        },
    )
}

pub fn num_to_string(n: f64) -> String {
    if n.is_nan() {
        "NaN".into()
    } else if n.is_infinite() {
        if n > 0.0 { "Infinity" } else { "-Infinity" }.into()
    } else if n == 0.0 {
        "0".into()
    } else {
        format!("{n}")
    }
}

fn str_to_num(s: &str) -> f64 {
    let t = s.trim_matches(|c| matches!(c, ' ' | '\t' | '\n' | '\r'));
    let body = t.strip_prefix('-').unwrap_or(t);
    let ok = !body.is_empty()
        && body != "."
        && body.chars().all(|c| c.is_ascii_digit() || c == '.')
        && body.matches('.').count() <= 1;
    if ok {
        t.parse().unwrap_or(f64::NAN)
    } else {
        f64::NAN
    }
}

fn xpath_round(n: f64) -> f64 {
    if n.is_nan() || n.is_infinite() {
        n
    } else {
        (n + 0.5).floor()
    }
}

impl Eval<'_> {
    fn tick(&self, n: usize) -> Result<(), String> {
        let before = self.ticks.get();
        let t = before + n as u64 + 1;
        self.ticks.set(t);
        if t > MAX_TICKS {
            return Err("expression is too expensive to evaluate".into());
        }
        if before / 4096 != t / 4096 {
            self.deadline.check().map_err(str::to_string)?;
        }
        Ok(())
    }

    fn sorted(mut v: Vec<NodeRef>) -> Vec<NodeRef> {
        v.sort_unstable();
        v.dedup();
        v
    }

    fn string_of(&self, v: &Val) -> String {
        match v {
            Val::Str(s) => s.clone(),
            Val::Num(n) => num_to_string(*n),
            Val::Bool(b) => b.to_string(),
            Val::Nodes(ns) => ns
                .iter()
                .min()
                .map(|n| self.dom.string_value(*n))
                .unwrap_or_default(),
        }
    }

    fn num_of(&self, v: &Val) -> f64 {
        match v {
            Val::Num(n) => *n,
            Val::Bool(b) => f64::from(*b as u8),
            other => str_to_num(&self.string_of(other)),
        }
    }

    fn bool_of(v: &Val) -> bool {
        match v {
            Val::Bool(b) => *b,
            Val::Num(n) => *n != 0.0 && !n.is_nan(),
            Val::Str(s) => !s.is_empty(),
            Val::Nodes(n) => !n.is_empty(),
        }
    }

    fn eval(&self, e: &Expr, c: Ctx) -> Result<Val, String> {
        self.tick(0)?;
        Ok(match e {
            Expr::Num(n) => Val::Num(*n),
            Expr::Str(s) => Val::Str(s.clone()),
            Expr::Var(v) => self
                .vars
                .get(v)
                .cloned()
                .ok_or_else(|| format!("undefined variable ${v}"))?,
            Expr::Neg(x) => Val::Num(-self.num_of(&self.eval(x, c)?)),
            Expr::Bin(op, l, r) => self.binary(*op, l, r, c)?,
            Expr::Union(l, r) => {
                let (Val::Nodes(mut a), Val::Nodes(b)) = (self.eval(l, c)?, self.eval(r, c)?)
                else {
                    return Err("'|' needs node-sets on both sides".into());
                };
                a.extend(b);
                Val::Nodes(Self::sorted(a))
            }
            Expr::Call(name, args) => self.call(name, args, c)?,
            Expr::Filter(inner, preds) => {
                let Val::Nodes(ns) = self.eval(inner, c)? else {
                    return Err("a predicate needs a node-set".into());
                };
                let mut ns = Self::sorted(ns);
                for p in preds {
                    ns = self.apply_pred(ns, p)?;
                }
                Val::Nodes(ns)
            }
            Expr::Path {
                start,
                absolute,
                steps,
            } => {
                let mut set = match start {
                    Some(s) => match self.eval(s, c)? {
                        Val::Nodes(n) => n,
                        _ => return Err("'/' needs a node-set on its left".into()),
                    },
                    None if *absolute => vec![NodeRef { id: 0, attr: 0 }],
                    None => vec![c.node],
                };
                for step in steps {
                    let mut next = Vec::new();
                    for n in &set {
                        let mut cand = self.axis_nodes(step.axis, *n)?;
                        cand.retain(|x| self.matches(step.axis, &step.test, *x));
                        for p in &step.preds {
                            cand = self.apply_pred(cand, p)?;
                        }
                        next.extend(cand);
                    }
                    set = Self::sorted(next);
                }
                Val::Nodes(set)
            }
        })
    }

    fn apply_pred(&self, nodes: Vec<NodeRef>, pred: &Expr) -> Result<Vec<NodeRef>, String> {
        let size = nodes.len();
        let mut out = Vec::new();
        for (i, n) in nodes.into_iter().enumerate() {
            let v = self.eval(
                pred,
                Ctx {
                    node: n,
                    pos: i + 1,
                    size,
                },
            )?;
            let keep = match v {
                Val::Num(x) => x == (i + 1) as f64,
                other => Self::bool_of(&other),
            };
            if keep {
                out.push(n);
            }
        }
        Ok(out)
    }

    fn matches(&self, axis: Axis, test: &Test, n: NodeRef) -> bool {
        let kind = self.dom.kind(n);
        let principal = if axis == Axis::Attribute {
            Kind::Attr
        } else {
            Kind::Elem
        };
        match test {
            Test::Node => true,
            Test::Text => kind == Kind::Text,
            Test::Comment => kind == Kind::Comment,
            Test::Pi => false,
            Test::Star => kind == principal,
            Test::PrefixStar(p) => {
                kind == principal
                    && self
                        .dom
                        .name(n)
                        .split_once(':')
                        .is_some_and(|(a, _)| a == p)
            }
            Test::Name(name) => kind == principal && self.dom.name(n) == name,
        }
    }

    /// Nodes on `axis` from `n`, in axis order (nearest first for reverse axes).
    fn axis_nodes(&self, axis: Axis, n: NodeRef) -> Result<Vec<NodeRef>, String> {
        let dom = self.dom;
        let is_attr = n.attr != 0;
        let plain = |id: u32| NodeRef { id, attr: 0 };
        let out: Vec<NodeRef> = match axis {
            Axis::Itself => vec![n],
            Axis::Child => {
                if is_attr {
                    Vec::new()
                } else {
                    dom.children(n.id).iter().map(|c| plain(*c)).collect()
                }
            }
            Axis::Descendant | Axis::DescendantOrSelf => {
                let mut v = Vec::new();
                if axis == Axis::DescendantOrSelf {
                    v.push(n);
                }
                if !is_attr {
                    v.extend((n.id + 1..dom.end(n.id)).map(plain));
                }
                v
            }
            Axis::Parent => {
                if is_attr {
                    vec![plain(n.id)]
                } else {
                    dom.parent(n.id).map(plain).into_iter().collect()
                }
            }
            Axis::Ancestor | Axis::AncestorOrSelf => {
                let mut v = Vec::new();
                if axis == Axis::AncestorOrSelf {
                    v.push(n);
                }
                let mut cur = if is_attr {
                    Some(n.id)
                } else {
                    dom.parent(n.id)
                };
                while let Some(id) = cur {
                    v.push(plain(id));
                    cur = dom.parent(id);
                }
                v
            }
            Axis::FollowingSibling | Axis::PrecedingSibling => {
                if is_attr {
                    return Ok(Vec::new());
                }
                let Some(p) = dom.parent(n.id) else {
                    return Ok(Vec::new());
                };
                let sibs = dom.children(p);
                let i = sibs.iter().position(|s| *s == n.id).unwrap_or(0);
                if axis == Axis::FollowingSibling {
                    sibs[i + 1..].iter().map(|c| plain(*c)).collect()
                } else {
                    sibs[..i].iter().rev().map(|c| plain(*c)).collect()
                }
            }
            Axis::Following => {
                let from = if is_attr { n.id + 1 } else { dom.end(n.id) };
                (from..dom.len()).map(plain).collect()
            }
            Axis::Preceding => (0..n.id)
                .rev()
                .filter(|i| dom.end(*i) <= n.id)
                .map(plain)
                .collect(),
            Axis::Attribute => {
                if is_attr {
                    Vec::new()
                } else {
                    dom.attrs(n.id)
                        .iter()
                        .enumerate()
                        .filter(|(_, (k, _))| k != "xmlns" && !k.starts_with("xmlns:"))
                        .map(|(i, _)| NodeRef {
                            id: n.id,
                            attr: i as u32 + 1,
                        })
                        .collect()
                }
            }
            Axis::Namespace => Vec::new(),
        };
        self.tick(out.len())?;
        Ok(out)
    }

    fn compare(&self, op: Op, a: &Val, b: &Val) -> bool {
        let rel = matches!(op, Op::Lt | Op::Le | Op::Gt | Op::Ge);
        let num_cmp = |x: f64, y: f64| match op {
            Op::Eq => x == y,
            Op::Neq => x != y,
            Op::Lt => x < y,
            Op::Le => x <= y,
            Op::Gt => x > y,
            _ => x >= y,
        };
        let str_cmp = |x: &str, y: &str| {
            if op == Op::Eq {
                x == y
            } else {
                x != y
            }
        };
        match (a, b) {
            (Val::Nodes(x), Val::Nodes(y)) => {
                let ys: Vec<String> = y.iter().map(|n| self.dom.string_value(*n)).collect();
                x.iter().any(|n| {
                    let s = self.dom.string_value(*n);
                    ys.iter().any(|t| {
                        if rel {
                            num_cmp(str_to_num(&s), str_to_num(t))
                        } else {
                            str_cmp(&s, t)
                        }
                    })
                })
            }
            (Val::Nodes(x), other) | (other, Val::Nodes(x)) => {
                let flipped = matches!(b, Val::Nodes(_));
                let ord = |l: f64, r: f64| {
                    if flipped {
                        num_cmp(r, l)
                    } else {
                        num_cmp(l, r)
                    }
                };
                match other {
                    Val::Bool(o) => ord(f64::from(!x.is_empty() as u8), f64::from(*o as u8)),
                    Val::Num(o) => x
                        .iter()
                        .any(|n| ord(str_to_num(&self.dom.string_value(*n)), *o)),
                    Val::Str(o) => x.iter().any(|n| {
                        let s = self.dom.string_value(*n);
                        if rel {
                            ord(str_to_num(&s), str_to_num(o))
                        } else {
                            str_cmp(&s, o)
                        }
                    }),
                    Val::Nodes(_) => unreachable!(),
                }
            }
            _ => {
                if !rel && (matches!(a, Val::Bool(_)) || matches!(b, Val::Bool(_))) {
                    let (x, y) = (Self::bool_of(a), Self::bool_of(b));
                    return if op == Op::Eq { x == y } else { x != y };
                }
                if rel || matches!(a, Val::Num(_)) || matches!(b, Val::Num(_)) {
                    return num_cmp(self.num_of(a), self.num_of(b));
                }
                str_cmp(&self.string_of(a), &self.string_of(b))
            }
        }
    }

    fn binary(&self, op: Op, l: &Expr, r: &Expr, c: Ctx) -> Result<Val, String> {
        match op {
            Op::Or => {
                let a = Self::bool_of(&self.eval(l, c)?);
                Ok(Val::Bool(a || Self::bool_of(&self.eval(r, c)?)))
            }
            Op::And => {
                let a = Self::bool_of(&self.eval(l, c)?);
                Ok(Val::Bool(a && Self::bool_of(&self.eval(r, c)?)))
            }
            Op::Eq | Op::Neq | Op::Lt | Op::Le | Op::Gt | Op::Ge => {
                let (a, b) = (self.eval(l, c)?, self.eval(r, c)?);
                Ok(Val::Bool(self.compare(op, &a, &b)))
            }
            _ => {
                let a = self.num_of(&self.eval(l, c)?);
                let b = self.num_of(&self.eval(r, c)?);
                Ok(Val::Num(match op {
                    Op::Add => a + b,
                    Op::Sub => a - b,
                    Op::Mul => a * b,
                    Op::Div => a / b,
                    _ => a % b,
                }))
            }
        }
    }

    fn call(&self, name: &str, args: &[Expr], c: Ctx) -> Result<Val, String> {
        let arity = |min: usize, max: usize| -> Result<(), String> {
            if args.len() < min || args.len() > max {
                Err(format!("wrong number of arguments to {name}()"))
            } else {
                Ok(())
            }
        };
        let arg = |i: usize| self.eval(&args[i], c);
        // String argument defaulting to the context node's string-value.
        let sarg = |i: usize| -> Result<String, String> {
            if args.len() > i {
                Ok(self.string_of(&arg(i)?))
            } else {
                Ok(self.dom.string_value(c.node))
            }
        };
        let nodes_arg = |i: usize| -> Result<Vec<NodeRef>, String> {
            match arg(i)? {
                Val::Nodes(n) => Ok(n),
                _ => Err(format!("{name}() needs a node-set")),
            }
        };
        let first_or_ctx = |i: usize| -> Result<Option<NodeRef>, String> {
            if args.len() > i {
                Ok(nodes_arg(i)?.into_iter().min())
            } else {
                Ok(Some(c.node))
            }
        };
        Ok(match name {
            "last" => {
                arity(0, 0)?;
                Val::Num(c.size as f64)
            }
            "position" => {
                arity(0, 0)?;
                Val::Num(c.pos as f64)
            }
            "count" => {
                arity(1, 1)?;
                Val::Num(nodes_arg(0)?.len() as f64)
            }
            "local-name" => {
                arity(0, 1)?;
                Val::Str(
                    first_or_ctx(0)?
                        .map(|n| self.dom.local_name(n))
                        .unwrap_or_default(),
                )
            }
            "name" => {
                arity(0, 1)?;
                Val::Str(
                    first_or_ctx(0)?
                        .map(|n| self.dom.name(n).to_string())
                        .unwrap_or_default(),
                )
            }
            "namespace-uri" => {
                arity(0, 1)?;
                Val::Str(
                    first_or_ctx(0)?
                        .map(|n| self.dom.ns_uri(n))
                        .unwrap_or_default(),
                )
            }
            "string" => {
                arity(0, 1)?;
                Val::Str(sarg(0)?)
            }
            "concat" => {
                if args.len() < 2 {
                    return Err("concat() needs at least two arguments".into());
                }
                let mut s = String::new();
                for i in 0..args.len() {
                    s.push_str(&self.string_of(&arg(i)?));
                }
                Val::Str(s)
            }
            "starts-with" => {
                arity(2, 2)?;
                Val::Bool(sarg(0)?.starts_with(&sarg(1)?))
            }
            "contains" => {
                arity(2, 2)?;
                Val::Bool(sarg(0)?.contains(&sarg(1)?))
            }
            "substring-before" => {
                arity(2, 2)?;
                let (s, t) = (sarg(0)?, sarg(1)?);
                Val::Str(s.find(&t).map(|i| s[..i].to_string()).unwrap_or_default())
            }
            "substring-after" => {
                arity(2, 2)?;
                let (s, t) = (sarg(0)?, sarg(1)?);
                Val::Str(
                    s.find(&t)
                        .map(|i| s[i + t.len()..].to_string())
                        .unwrap_or_default(),
                )
            }
            "substring" => {
                arity(2, 3)?;
                let s = sarg(0)?;
                let start = xpath_round(self.num_of(&arg(1)?));
                let end = if args.len() == 3 {
                    start + xpath_round(self.num_of(&arg(2)?))
                } else {
                    f64::INFINITY
                };
                Val::Str(
                    s.chars()
                        .enumerate()
                        .filter(|(i, _)| {
                            let p = (*i + 1) as f64;
                            p >= start && p < end
                        })
                        .map(|(_, ch)| ch)
                        .collect(),
                )
            }
            "string-length" => {
                arity(0, 1)?;
                Val::Num(sarg(0)?.chars().count() as f64)
            }
            "normalize-space" => {
                arity(0, 1)?;
                Val::Str(sarg(0)?.split_whitespace().collect::<Vec<_>>().join(" "))
            }
            "translate" => {
                arity(3, 3)?;
                let (s, from, to) = (sarg(0)?, sarg(1)?, sarg(2)?);
                let from: Vec<char> = from.chars().collect();
                let to: Vec<char> = to.chars().collect();
                Val::Str(
                    s.chars()
                        .filter_map(|ch| match from.iter().position(|f| *f == ch) {
                            None => Some(ch),
                            Some(i) => to.get(i).copied(),
                        })
                        .collect(),
                )
            }
            "boolean" => {
                arity(1, 1)?;
                Val::Bool(Self::bool_of(&arg(0)?))
            }
            "not" => {
                arity(1, 1)?;
                Val::Bool(!Self::bool_of(&arg(0)?))
            }
            "true" => {
                arity(0, 0)?;
                Val::Bool(true)
            }
            "false" => {
                arity(0, 0)?;
                Val::Bool(false)
            }
            "number" => {
                arity(0, 1)?;
                if args.is_empty() {
                    Val::Num(str_to_num(&self.dom.string_value(c.node)))
                } else {
                    Val::Num(self.num_of(&arg(0)?))
                }
            }
            "sum" => {
                arity(1, 1)?;
                Val::Num(
                    nodes_arg(0)?
                        .iter()
                        .map(|n| str_to_num(&self.dom.string_value(*n)))
                        .sum(),
                )
            }
            "floor" => {
                arity(1, 1)?;
                Val::Num(self.num_of(&arg(0)?).floor())
            }
            "ceiling" => {
                arity(1, 1)?;
                Val::Num(self.num_of(&arg(0)?).ceil())
            }
            "round" => {
                arity(1, 1)?;
                Val::Num(xpath_round(self.num_of(&arg(0)?)))
            }
            other => return Err(format!("unsupported function {other}()")),
        })
    }
}
