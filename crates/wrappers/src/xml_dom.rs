//! `native.xml.parse(source, opts)`: builds an in-memory node tree and queries it with XPath 1.0.
//!
//! The tree and the XPath engine come from the `uppsala` crate. The whole document is read first
//! (capped at the object size limit) and parsed with DTDs refused; larger inputs belong to the
//! `native.xml.events` pull parser.

use std::{io::Read, sync::Arc};

use crate::{
    xml::{source_reader, Limited},
    Deadline,
};
use mlua::{Lua, MetaMethod, Table, UserData, UserDataMethods, UserDataRef, Value};
use uppsala::{dom::NodeKind, Document, NodeId, Parser, XPathEvaluator, XPathValue};

const MAX_DEPTH: u32 = 256;
/// Axis and predicate node visits allowed for one XPath evaluation.
const MAX_XPATH_VISITS: usize = 5_000_000;
const MAX_XPATH_DEPTH: u32 = 64;

fn xml_err(e: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(format!("xml: {e}"))
}

/// Drops whitespace-only text nodes and trims the rest, like `trim = true` for `events`.
fn trim_text(doc: &mut Document<'static>) {
    let mut edits = Vec::new();
    for id in doc.descendants(doc.root()) {
        if let Some(NodeKind::Text(t)) = doc.node_kind(id) {
            let trimmed = t.trim();
            if trimmed.is_empty() {
                edits.push((id, None));
            } else if trimmed.len() != t.len() {
                edits.push((id, Some(trimmed.to_string())));
            }
        }
    }
    for (id, new) in edits {
        match new {
            None => doc.detach(id),
            Some(s) => {
                if let Some(NodeKind::Text(t)) = doc.node_kind_mut(id) {
                    *t = s.into();
                }
            }
        }
    }
}

fn build(
    reader: Box<dyn Read + Send>,
    max_bytes: usize,
    deadline: &Deadline,
    trim: bool,
) -> mlua::Result<Document<'static>> {
    let mut bytes = Vec::new();
    // One byte of slack tells "exactly at the limit" from "over it".
    Limited {
        inner: reader,
        left: max_bytes as u64 + 1,
        deadline: deadline.clone(),
    }
    .read_to_end(&mut bytes)
    .map_err(xml_err)?;
    if bytes.len() > max_bytes {
        return Err(xml_err(
            "document exceeds the object size limit; use native.xml.events",
        ));
    }
    let text = String::from_utf8(bytes).map_err(|_| xml_err("document is not valid UTF-8"))?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    let mut doc = Parser::new()
        .with_max_depth(MAX_DEPTH)
        .with_forbid_dtd(true)
        .parse(text)
        .map_err(xml_err)?
        .into_static();
    if doc.document_element().is_none() {
        return Err(xml_err("document has no root element"));
    }
    if trim {
        trim_text(&mut doc);
    }
    doc.prepare_xpath();
    Ok(doc)
}

#[derive(Clone)]
struct XmlNode {
    doc: Arc<Document<'static>>,
    id: NodeId,
    deadline: Deadline,
}

impl XmlNode {
    fn wrap(&self, id: NodeId) -> XmlNode {
        XmlNode {
            doc: self.doc.clone(),
            id,
            deadline: self.deadline.clone(),
        }
    }

    fn name(&self) -> String {
        match self.doc.node_kind(self.id) {
            Some(NodeKind::Element(e)) => e.name.prefixed_name().into_owned(),
            Some(NodeKind::Attribute(q, _)) => q.prefixed_name().into_owned(),
            _ => String::new(),
        }
    }

    fn qname(&self) -> Option<&uppsala::QName<'static>> {
        match self.doc.node_kind(self.id) {
            Some(NodeKind::Element(e)) => Some(&e.name),
            Some(NodeKind::Attribute(q, _)) => Some(q),
            _ => None,
        }
    }

    fn text(&self) -> String {
        XPathValue::NodeSet(vec![self.id]).to_string_value(&self.doc)
    }

    fn element_children(&self, name: Option<&str>) -> Vec<XmlNode> {
        self.doc
            .children_iter(self.id)
            .filter_map(|c| match self.doc.node_kind(c) {
                Some(NodeKind::Element(e)) if name.is_none_or(|n| e.name.prefixed_name() == n) => {
                    Some(self.wrap(c))
                }
                _ => None,
            })
            .collect()
    }

    fn query(&self, expr: &str, ns: Option<Table>) -> mlua::Result<XPathValue> {
        self.deadline.check().map_err(mlua::Error::runtime)?;
        let mut eval = XPathEvaluator::new()
            .with_max_node_visits(MAX_XPATH_VISITS)
            .with_max_depth(MAX_XPATH_DEPTH);
        // Prefixes declared on the document element are usable in expressions as written.
        if let Some(NodeKind::Element(root)) = self
            .doc
            .document_element()
            .and_then(|r| self.doc.node_kind(r))
        {
            for (prefix, uri) in &root.namespace_declarations {
                if !prefix.is_empty() {
                    eval.add_namespace(prefix.to_string(), uri.to_string());
                }
            }
        }
        if let Some(ns) = ns {
            for pair in ns.pairs::<String, String>() {
                let (prefix, uri) = pair?;
                eval.add_namespace(prefix, uri);
            }
        }
        let v = eval
            .evaluate(&self.doc, self.id, expr)
            .map_err(|e| xml_err(format!("xpath: {e}")))?;
        self.deadline.check().map_err(mlua::Error::runtime)?;
        Ok(v)
    }

    fn to_lua(&self, lua: &Lua, v: XPathValue) -> mlua::Result<Value> {
        Ok(match v {
            XPathValue::String(s) => Value::String(lua.create_string(s)?),
            XPathValue::Number(n) => Value::Number(n),
            XPathValue::Boolean(b) => Value::Boolean(b),
            XPathValue::NodeSet(ns) => {
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
        // "document", "element", "text", "comment", "processing-instruction" or "attribute".
        methods.add_method("kind", |_, this, ()| {
            Ok(match this.doc.node_kind(this.id) {
                Some(NodeKind::Document) | None => "document",
                Some(NodeKind::Element(_)) => "element",
                Some(NodeKind::Text(_) | NodeKind::CData(_)) => "text",
                Some(NodeKind::Comment(_)) => "comment",
                Some(NodeKind::ProcessingInstruction(_)) => "processing-instruction",
                Some(NodeKind::Attribute(..)) => "attribute",
            })
        });
        // Qualified name of an element or attribute, "" for other nodes.
        methods.add_method("name", |_, this, ()| Ok(this.name()));
        methods.add_method("local_name", |_, this, ()| {
            Ok(this
                .qname()
                .map(|q| q.local_name.to_string())
                .unwrap_or_default())
        });
        methods.add_method("namespace", |_, this, ()| {
            Ok(this
                .qname()
                .and_then(|q| q.namespace_uri.as_ref().map(|u| u.to_string()))
                .unwrap_or_default())
        });
        // XPath string-value: all descendant text, concatenated.
        methods.add_method("text", |_, this, ()| Ok(this.text()));
        methods.add_method("attr", |_, this, name: String| {
            Ok(match this.doc.node_kind(this.id) {
                Some(NodeKind::Element(e)) => e
                    .attributes
                    .iter()
                    .find(|a| a.name.prefixed_name() == name)
                    .map(|a| a.value.to_string()),
                _ => None,
            })
        });
        methods.add_method("attrs", |lua, this, ()| {
            let t = lua.create_table()?;
            if let Some(NodeKind::Element(e)) = this.doc.node_kind(this.id) {
                for a in &e.attributes {
                    t.set(a.name.prefixed_name().as_ref(), a.value.as_ref())?;
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
            Ok(this.doc.document_element().map(|r| this.wrap(r)))
        });
        methods.add_method("parent", |_, this, ()| {
            Ok(this.doc.parent(this.id).map(|p| this.wrap(p)))
        });
        // Node-sets come back as arrays of nodes in document order; other results as a Lua
        // string, number or boolean. `ns` maps extra prefixes to namespace URIs.
        methods.add_method("xpath", |lua, this, (expr, ns): (String, Option<Table>)| {
            let v = this.query(&expr, ns)?;
            this.to_lua(lua, v)
        });
        // Like xpath, but a node-set yields only its first node (nil when empty).
        methods.add_method(
            "first",
            |lua, this, (expr, ns): (String, Option<Table>)| match this.query(&expr, ns)? {
                XPathValue::NodeSet(ns) => Ok(match ns.first() {
                    Some(n) => Value::UserData(lua.create_userdata(this.wrap(*n))?),
                    None => Value::Nil,
                }),
                other => this.to_lua(lua, other),
            },
        );
        // Serializes the node and its subtree back to XML text.
        methods.add_method("xml", |lua, this, ()| match this.doc.node_kind(this.id) {
            Some(NodeKind::Attribute(..)) => lua.create_string(this.text()),
            _ => lua.create_string(this.doc.node_to_xml(this.id)),
        });
        methods.add_meta_method(MetaMethod::Eq, |_, this, other: UserDataRef<XmlNode>| {
            Ok(Arc::ptr_eq(&this.doc, &other.doc) && this.id == other.id)
        });
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            Ok(match this.doc.node_kind(this.id) {
                Some(NodeKind::Element(_) | NodeKind::Attribute(..)) => {
                    format!("XmlNode({})", this.name())
                }
                Some(NodeKind::Text(_) | NodeKind::CData(_)) => "XmlNode(text)".to_string(),
                Some(NodeKind::Comment(_)) => "XmlNode(comment)".to_string(),
                Some(NodeKind::ProcessingInstruction(_)) => "XmlNode(pi)".to_string(),
                _ => "XmlNode(document)".to_string(),
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
            let doc = build(reader, max_object_bytes, &deadline, trim)?;
            let id = doc.root();
            Ok(XmlNode {
                doc: Arc::new(doc),
                id,
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
        <feed xmlns:m="urn:m">
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
            assert(root:namespace() == "")
            local items = root:children("item")
            assert(#items == 3)
            assert(items[1]:attr("id") == "1" and items[1]:attrs().kind == "a")
            assert(items[1]:child("name"):text() == "One & only")
            assert(items[1]:children()[2]:local_name() == "price")
            assert(items[1]:children()[2]:name() == "m:price")
            assert(items[1]:children()[2]:namespace() == "urn:m")
            assert(items[2]:parent() == root)
            assert(items[3]:text() == "Three7x<y")
            assert(doc:kind() == "document" and doc:root() == root)
            assert(items[2]:xml():find("<name>Two</name>", 1, true))
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
            assert(doc:first("//item[@kind='b']/name"):text() == "Two")
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
    fn default_namespace_needs_a_prefix_binding() {
        let ok: bool = lua()
            .load(
                r#"
            local doc = xml.parse('<feed xmlns="urn:f"><item/><item/></feed>')
            assert(#doc:xpath("//item") == 0)
            assert(#doc:xpath("//f:item", { f = "urn:f" }) == 2)
            assert(#doc:xpath("//*[local-name()='item']") == 2)
            assert(doc:root():namespace() == "urn:f")
            return true"#,
            )
            .eval()
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn errors_and_limits() {
        let l = lua();
        for code in [
            r#"xml.parse("<a><b></a>")"#,
            r#"xml.parse("<a/><b/>")"#,
            r#"xml.parse("")"#,
            r#"xml.parse("<!DOCTYPE a [<!ENTITY e 'x'>]><a>&e;</a>")"#,
            r#"xml.parse("<a/>"):xpath("//[")"#,
            r#"xml.parse("<a/>"):xpath("nope()")"#,
            r#"xml.parse("<a/>"):xpath("$x")"#,
            // Expensive expressions hit the evaluation budget instead of running for ages.
            r#"xml.parse("<r>" .. string.rep("<a><a><a/></a></a>", 3000) .. "</r>"):xpath("count(//*[count(//*[count(//*) > 0]) > 0])")"#,
            r#"xml.parse(string.rep("<a>", 300) .. string.rep("</a>", 300))"#,
            r#"xml.parse("<a>" .. string.rep("x", 1100000) .. "</a>")"#,
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
