//! `native.xml.parse(source, opts)`: builds an in-memory node tree and queries it with XPath 1.0.
//!
//! The tree is an arena in document order, so a node's subtree is the id range `id..end` and
//! document order is plain id order. Attributes are not arena nodes; an attribute node is the
//! pair (element id, attribute index + 1). The parse is bounded by the object size limit and a
//! node count; for anything bigger use the `native.xml.events` pull parser.

use std::{collections::HashMap, sync::Arc};

use crate::{
    xml::{read_logical, source_reader, Ev, State},
    xml_writer::escape,
    xpath::{self, Val},
    Deadline,
};
use mlua::{Lua, MetaMethod, Table, UserData, UserDataMethods, UserDataRef, Value};

const MAX_DEPTH: usize = 256;
const MAX_NODES: usize = 2_000_000;
const XML_NS: &str = "http://www.w3.org/XML/1998/namespace";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Doc,
    Elem,
    Text,
    Comment,
    Attr,
}

/// A node, or an attribute of the element `id` (`attr` is its index + 1, 0 for none).
/// Ordering is document order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct NodeRef {
    pub id: u32,
    pub attr: u32,
}

struct Node {
    kind: Kind,
    /// Qualified name of an element.
    name: String,
    /// Content of a text or comment node.
    value: String,
    attrs: Vec<(String, String)>,
    parent: Option<u32>,
    children: Vec<u32>,
    /// One past the last id in this node's subtree.
    end: u32,
}

pub struct Dom {
    nodes: Vec<Node>,
}

impl Dom {
    pub fn len(&self) -> u32 {
        self.nodes.len() as u32
    }

    pub fn end(&self, id: u32) -> u32 {
        self.nodes[id as usize].end
    }

    pub fn parent(&self, id: u32) -> Option<u32> {
        self.nodes[id as usize].parent
    }

    pub fn children(&self, id: u32) -> &[u32] {
        &self.nodes[id as usize].children
    }

    pub fn attrs(&self, id: u32) -> &[(String, String)] {
        &self.nodes[id as usize].attrs
    }

    pub fn kind(&self, n: NodeRef) -> Kind {
        if n.attr != 0 {
            Kind::Attr
        } else {
            self.nodes[n.id as usize].kind
        }
    }

    /// Qualified name of an element or attribute; empty for other nodes.
    pub fn name(&self, n: NodeRef) -> &str {
        if n.attr != 0 {
            &self.nodes[n.id as usize].attrs[n.attr as usize - 1].0
        } else if self.nodes[n.id as usize].kind == Kind::Elem {
            &self.nodes[n.id as usize].name
        } else {
            ""
        }
    }

    pub fn local_name(&self, n: NodeRef) -> String {
        let name = self.name(n);
        name.rsplit_once(':').map_or(name, |(_, l)| l).to_string()
    }

    /// Namespace URI an element or attribute name resolves to, or "".
    pub fn ns_uri(&self, n: NodeRef) -> String {
        let kind = self.dom_kind(n);
        if !matches!(kind, Kind::Elem | Kind::Attr) {
            return String::new();
        }
        let name = self.name(n);
        let (prefix, decl) = match name.split_once(':') {
            Some(("xml", _)) => return XML_NS.to_string(),
            Some((p, _)) => (Some(p), format!("xmlns:{p}")),
            // Unprefixed attributes are in no namespace.
            None if kind == Kind::Attr => return String::new(),
            None => (None, "xmlns".to_string()),
        };
        let _ = prefix;
        let mut cur = Some(n.id);
        while let Some(id) = cur {
            if let Some((_, v)) = self.nodes[id as usize]
                .attrs
                .iter()
                .find(|(k, _)| *k == decl)
            {
                return v.clone();
            }
            cur = self.nodes[id as usize].parent;
        }
        String::new()
    }

    fn dom_kind(&self, n: NodeRef) -> Kind {
        self.kind(n)
    }

    /// XPath string-value: the concatenated text of a node and its descendants.
    pub fn string_value(&self, n: NodeRef) -> String {
        if n.attr != 0 {
            return self.nodes[n.id as usize].attrs[n.attr as usize - 1]
                .1
                .clone();
        }
        let node = &self.nodes[n.id as usize];
        match node.kind {
            Kind::Text | Kind::Comment => node.value.clone(),
            _ => self.nodes[n.id as usize + 1..node.end as usize]
                .iter()
                .filter(|d| d.kind == Kind::Text)
                .map(|d| d.value.as_str())
                .collect(),
        }
    }

    fn serialize(&self, id: u32, out: &mut Vec<u8>) {
        let node = &self.nodes[id as usize];
        match node.kind {
            Kind::Text => escape(out, &node.value, false),
            Kind::Comment => {
                out.extend_from_slice(b"<!--");
                out.extend_from_slice(node.value.as_bytes());
                out.extend_from_slice(b"-->");
            }
            Kind::Doc => {
                for c in &node.children {
                    self.serialize(*c, out);
                }
            }
            _ => {
                out.push(b'<');
                out.extend_from_slice(node.name.as_bytes());
                for (k, v) in &node.attrs {
                    out.push(b' ');
                    out.extend_from_slice(k.as_bytes());
                    out.extend_from_slice(b"=\"");
                    escape(out, v, true);
                    out.push(b'"');
                }
                if node.children.is_empty() {
                    out.extend_from_slice(b"/>");
                } else {
                    out.push(b'>');
                    for c in &node.children {
                        self.serialize(*c, out);
                    }
                    out.extend_from_slice(b"</");
                    out.extend_from_slice(node.name.as_bytes());
                    out.push(b'>');
                }
            }
        }
    }
}

fn xml_err(e: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("xml: {e}"))
}

fn build(mut st: State) -> mlua::Result<Dom> {
    let mut nodes = vec![Node {
        kind: Kind::Doc,
        name: String::new(),
        value: String::new(),
        attrs: Vec::new(),
        parent: None,
        children: Vec::new(),
        end: 0,
    }];
    let mut stack: Vec<u32> = vec![0];
    let mut root_seen = false;
    let add = |nodes: &mut Vec<Node>, parent: u32, node: Node| -> mlua::Result<u32> {
        if nodes.len() >= MAX_NODES {
            return Err(xml_err("document has too many nodes for native.xml.parse"));
        }
        let id = nodes.len() as u32;
        nodes.push(Node {
            parent: Some(parent),
            ..node
        });
        nodes[parent as usize].children.push(id);
        Ok(id)
    };
    let blank = |kind, name: String, value: String| Node {
        kind,
        name,
        value,
        attrs: Vec::new(),
        parent: None,
        children: Vec::new(),
        end: 0,
    };
    loop {
        let top = *stack.last().unwrap();
        match read_logical(&mut st)? {
            Ev::Start { tag, attrs, empty } => {
                if top == 0 {
                    if root_seen {
                        return Err(xml_err("document has more than one root element"));
                    }
                    root_seen = true;
                }
                if stack.len() > MAX_DEPTH {
                    return Err(xml_err("nesting too deep"));
                }
                let mut node = blank(Kind::Elem, tag, String::new());
                node.attrs = attrs;
                let id = add(&mut nodes, top, node)?;
                if empty {
                    nodes[id as usize].end = id + 1;
                } else {
                    stack.push(id);
                }
            }
            Ev::End(_) => {
                let id = stack
                    .pop()
                    .filter(|i| *i != 0)
                    .ok_or_else(|| xml_err("unbalanced end tag"))?;
                nodes[id as usize].end = nodes.len() as u32;
            }
            Ev::Text(t) | Ev::CData(t) => {
                if top == 0 {
                    if !t.trim().is_empty() {
                        return Err(xml_err("text outside the root element"));
                    }
                    continue;
                }
                match nodes[top as usize].children.last().copied() {
                    Some(last) if nodes[last as usize].kind == Kind::Text => {
                        nodes[last as usize].value.push_str(&t)
                    }
                    _ => {
                        let id = add(&mut nodes, top, blank(Kind::Text, String::new(), t))?;
                        nodes[id as usize].end = id + 1;
                    }
                }
            }
            Ev::Comment(t) => {
                let id = add(&mut nodes, top, blank(Kind::Comment, String::new(), t))?;
                nodes[id as usize].end = id + 1;
            }
            Ev::Eof => break,
        }
    }
    if stack.len() != 1 {
        return Err(xml_err("unexpected end of document"));
    }
    if !root_seen {
        return Err(xml_err("document has no root element"));
    }
    nodes[0].end = nodes.len() as u32;
    Ok(Dom { nodes })
}

#[derive(Clone)]
struct XmlNode {
    dom: Arc<Dom>,
    r: NodeRef,
    deadline: Deadline,
}

impl XmlNode {
    fn wrap(&self, r: NodeRef) -> XmlNode {
        XmlNode {
            dom: self.dom.clone(),
            r,
            deadline: self.deadline.clone(),
        }
    }

    fn element_children(&self, name: Option<&str>) -> Vec<XmlNode> {
        if self.r.attr != 0 {
            return Vec::new();
        }
        self.dom
            .children(self.r.id)
            .iter()
            .map(|c| NodeRef { id: *c, attr: 0 })
            .filter(|c| self.dom.kind(*c) == Kind::Elem)
            .filter(|c| name.is_none_or(|n| self.dom.name(*c) == n))
            .map(|c| self.wrap(c))
            .collect()
    }

    fn query(&self, expr: &str, vars: Option<Table>) -> mlua::Result<Val> {
        let compiled = xpath::compile(expr).map_err(|e| xml_err(format!("xpath: {e}")))?;
        let mut v = HashMap::new();
        if let Some(t) = vars {
            for pair in t.pairs::<String, Value>() {
                let (k, val) = pair?;
                let val = match val {
                    Value::String(s) => Val::Str(
                        s.to_str()
                            .map_err(|_| xml_err("xpath variable is not valid UTF-8"))?
                            .to_string(),
                    ),
                    Value::Integer(i) => Val::Num(i as f64),
                    Value::Number(n) => Val::Num(n),
                    Value::Boolean(b) => Val::Bool(b),
                    _ => {
                        return Err(xml_err(format!(
                            "xpath variable ${k} must be a string, number or boolean"
                        )))
                    }
                };
                v.insert(k, val);
            }
        }
        xpath::eval(&self.dom, &compiled, self.r, &v, &self.deadline)
            .map_err(|e| xml_err(format!("xpath: {e}")))
    }

    fn to_lua(&self, lua: &Lua, v: Val) -> mlua::Result<Value> {
        Ok(match v {
            Val::Str(s) => Value::String(lua.create_string(s)?),
            Val::Num(n) => Value::Number(n),
            Val::Bool(b) => Value::Boolean(b),
            Val::Nodes(ns) => {
                let t = lua.create_table_with_capacity(ns.len(), 0)?;
                for (i, n) in ns.into_iter().enumerate() {
                    t.raw_set(i + 1, self.wrap(n))?;
                }
                Value::Table(t)
            }
        })
    }
}

impl UserData for XmlNode {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // "document", "element", "text", "comment" or "attribute".
        methods.add_method("kind", |_, this, ()| {
            Ok(match this.dom.kind(this.r) {
                Kind::Doc => "document",
                Kind::Elem => "element",
                Kind::Text => "text",
                Kind::Comment => "comment",
                Kind::Attr => "attribute",
            })
        });
        // Qualified name of an element or attribute, "" for other nodes.
        methods.add_method("name", |_, this, ()| Ok(this.dom.name(this.r).to_string()));
        methods.add_method("local_name", |_, this, ()| Ok(this.dom.local_name(this.r)));
        methods.add_method("namespace", |_, this, ()| Ok(this.dom.ns_uri(this.r)));
        // XPath string-value: all descendant text, concatenated.
        methods.add_method("text", |_, this, ()| Ok(this.dom.string_value(this.r)));
        methods.add_method("attr", |_, this, name: String| {
            if this.r.attr != 0 {
                return Ok(None);
            }
            Ok(this
                .dom
                .attrs(this.r.id)
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.clone()))
        });
        methods.add_method("attrs", |lua, this, ()| {
            let t = lua.create_table()?;
            if this.r.attr == 0 {
                for (k, v) in this.dom.attrs(this.r.id) {
                    t.set(k.as_str(), v.as_str())?;
                }
            }
            Ok(t)
        });
        // Element children, optionally only those with the given name.
        methods.add_method("children", |lua, this, name: Option<String>| {
            let kids = this.element_children(name.as_deref());
            let t = lua.create_table_with_capacity(kids.len(), 0)?;
            for (i, k) in kids.into_iter().enumerate() {
                t.raw_set(i + 1, k)?;
            }
            Ok(t)
        });
        // First element child with the given name.
        methods.add_method("child", |_, this, name: String| {
            Ok(this.element_children(Some(&name)).into_iter().next())
        });
        // The document element, from the document node.
        methods.add_method("root", |_, this, ()| {
            Ok(this
                .dom
                .children(0)
                .iter()
                .map(|c| NodeRef { id: *c, attr: 0 })
                .find(|c| this.dom.kind(*c) == Kind::Elem)
                .map(|c| this.wrap(c)))
        });
        methods.add_method("parent", |_, this, ()| {
            let p = if this.r.attr != 0 {
                Some(this.r.id)
            } else {
                this.dom.parent(this.r.id)
            };
            Ok(p.map(|id| this.wrap(NodeRef { id, attr: 0 })))
        });
        // Node-sets come back as arrays of nodes in document order; other results as a
        // Lua string, number or boolean. `vars` binds $name references.
        methods.add_method(
            "xpath",
            |lua, this, (expr, vars): (String, Option<Table>)| {
                let v = this.query(&expr, vars)?;
                this.to_lua(lua, v)
            },
        );
        // Like xpath, but a node-set yields only its first node (nil when empty).
        methods.add_method(
            "first",
            |lua, this, (expr, vars): (String, Option<Table>)| match this.query(&expr, vars)? {
                Val::Nodes(ns) => Ok(match ns.first() {
                    Some(n) => Value::UserData(lua.create_userdata(this.wrap(*n))?),
                    None => Value::Nil,
                }),
                other => this.to_lua(lua, other),
            },
        );
        // Serializes the node and its subtree back to XML text.
        methods.add_method("xml", |lua, this, ()| {
            if this.r.attr != 0 {
                return lua.create_string(this.dom.string_value(this.r));
            }
            let mut out = Vec::new();
            this.dom.serialize(this.r.id, &mut out);
            lua.create_string(out)
        });
        methods.add_meta_method(MetaMethod::Eq, |_, this, other: UserDataRef<XmlNode>| {
            Ok(Arc::ptr_eq(&this.dom, &other.dom) && this.r == other.r)
        });
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(match this.dom.kind(this.r) {
                Kind::Elem | Kind::Attr => format!("XmlNode({})", this.dom.name(this.r)),
                Kind::Doc => "XmlNode(document)".to_string(),
                Kind::Text => "XmlNode(text)".to_string(),
                Kind::Comment => "XmlNode(comment)".to_string(),
            })
        });
    }
}

pub fn api(
    lua: &Lua,
    xml: &Table,
    max_object_bytes: usize,
    deadline: Deadline,
) -> mlua::Result<()> {
    xml.set(
        "parse",
        lua.create_function(move |_, (source, opts): (Value, Option<Table>)| {
            let reader = source_reader(source, "native.xml.parse")?;
            let trim = opts
                .map(|o| o.get::<Option<bool>>("trim"))
                .transpose()?
                .flatten()
                .unwrap_or(false);
            let st = State::new(reader, max_object_bytes as u64, deadline.clone(), trim);
            let dom = build(st).map_err(|e| match e {
                mlua::Error::RuntimeError(m) if m.contains("stream limit") => {
                    xml_err("document exceeds the object size limit; use native.xml.events")
                }
                e => e,
            })?;
            Ok(XmlNode {
                dom: Arc::new(dom),
                r: NodeRef { id: 0, attr: 0 },
                deadline: deadline.clone(),
            })
        })?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn lua() -> Lua {
        let lua = Lua::new();
        let xml = lua.create_table().unwrap();
        let d = Deadline::new(Instant::now() + Duration::from_secs(30));
        api(&lua, &xml, 1 << 20, d.clone()).unwrap();
        crate::xml_writer::api(&lua, &xml, 1 << 20, d).unwrap();
        lua.globals().set("xml", xml).unwrap();
        lua
    }

    const DOC: &str = r#"
        local doc = xml.parse([==[<?xml version="1.0"?>
        <feed xmlns="urn:f" xmlns:m="urn:m">
          <!-- c -->
          <item id="1" kind="a"><name>One &amp; only</name><m:price>10.5</m:price></item>
          <item id="2" kind="b"><name>Two</name><m:price>3</m:price></item>
          <item id="3" kind="a"><name>Three</name><m:price>7</m:price><![CDATA[x<y]]></item>
        </feed>]==], { trim = true })
    "#;

    fn eval<T: mlua::FromLua>(body: &str) -> T {
        lua().load(format!("{DOC}\n{body}")).eval().unwrap()
    }

    #[test]
    fn navigation_and_accessors() {
        let ok: bool = eval(
            r#"
            local root = doc:root()
            assert(root:name() == "feed" and root:kind() == "element")
            assert(root:namespace() == "urn:f")
            local items = root:children("item")
            assert(#items == 3)
            assert(items[1]:attr("id") == "1" and items[1]:attrs().kind == "a")
            assert(items[1]:child("name"):text() == "One & only")
            assert(items[1]:children()[2]:local_name() == "price")
            assert(items[1]:children()[2]:namespace() == "urn:m")
            assert(items[2]:parent() == root)
            assert(items[3]:text() == "Three7x<y")
            assert(doc:kind() == "document" and doc:root() == root)
            assert(items[2]:xml() == '<item id="2" kind="b"><name>Two</name><m:price>3</m:price></item>')
            return true"#,
        );
        assert!(ok);
    }

    #[test]
    fn xpath_selection() {
        let ok: bool = eval(
            r#"
            assert(#doc:xpath("//item") == 3)
            assert(#doc:xpath("/feed/item[@kind='a']") == 2)
            assert(doc:xpath("/feed/item[2]/name")[1]:text() == "Two")
            assert(doc:xpath("/feed/item[last()]/@id")[1]:text() == "3")
            assert(doc:xpath("count(//item)") == 3)
            assert(doc:xpath("sum(//m:price)") == 20.5)
            assert(doc:xpath("string(//item[@id=2]/name)") == "Two")
            assert(doc:xpath("//item[m:price > 5]/@id")[2]:text() == "3")
            assert(#doc:xpath("//item[position() < 3]") == 2)
            assert(doc:xpath("boolean(//item[@id='9'])") == false)
            assert(doc:first("//item[@kind=$k]/name", { k = "b" }):text() == "Two")
            assert(doc:first("//nothing") == nil)
            assert(doc:xpath("concat(//item[1]/@id, '-', //item[2]/@id)") == "1-2")
            assert(doc:xpath("normalize-space('  a   b ')") == "a b")
            assert(doc:xpath("substring('12345', 2, 3)") == "234")
            assert(doc:xpath("translate('abc', 'ab', 'AB')") == "ABc")
            assert(doc:xpath("2 + 3 * 4 - 10 div 4 mod 2") == 13.5)
            assert(#doc:xpath("//item[@id=1]/following-sibling::item") == 2)
            assert(#doc:xpath("//item[@id=3]/preceding-sibling::item") == 2)
            assert(#doc:xpath("//name[. = 'Two']/ancestor::*") == 2)
            assert(#doc:xpath("//item[@id=1] | //item[@id=3]") == 2)
            assert(#doc:xpath("//comment()") == 1)
            assert(#doc:xpath("//item[@id=2]/following::name") == 1)
            assert(#doc:xpath("//item[@id=3]/preceding::name") == 2)
            assert(#doc:xpath("//*[local-name()='price']") == 3)
            assert(#doc:xpath("//m:*") == 3)
            assert(doc:xpath("name(/*)") == "feed")
            assert(doc:xpath("//item[1]/name/text()")[1]:kind() == "text")
            assert(doc:xpath("/feed/item[1]/@id")[1]:kind() == "attribute")
            local item = doc:xpath("//item[@id=2]")[1]
            assert(item:xpath("string(name)") == "Two" and item:xpath("string(../item[1]/@id)") == "1")
            return true"#,
        );
        assert!(ok);
    }

    #[test]
    fn errors() {
        let l = lua();
        for code in [
            r#"xml.parse("<a><b></a>")"#,
            r#"xml.parse("<a/><b/>")"#,
            r#"xml.parse("")"#,
            r#"xml.parse("<!DOCTYPE a [<!ENTITY e 'x'>]><a>&e;</a>")"#,
            r#"xml.parse("<a/>"):xpath("//[")"#,
            r#"xml.parse("<a/>"):xpath("nope()")"#,
            r#"xml.parse("<a/>"):xpath("$x")"#,
            r#"xml.parse("<a/>"):xpath("1 | 2")"#,
        ] {
            assert!(l.load(code).exec().is_err(), "should fail: {code}");
        }
    }

    #[test]
    fn round_trip_with_writer() {
        let s: String = lua()
            .load(
                r#"
            local w = xml.writer({ declaration = false })
            w:start("a", { n = "x&y" }); w:element("b", nil, "1 < 2"); w:close()
            local doc = xml.parse(w:finish())
            return doc:root():attr("n") .. "|" .. doc:xpath("string(//b)")"#,
            )
            .eval()
            .unwrap();
        assert_eq!(s, "x&y|1 < 2");
    }
}
