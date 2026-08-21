/**
 * interface → type codemod.
 *
 * Rewrites every `interface` declaration in the given files/directories as a
 * `type` alias, preserving JSDoc, modifiers, generics and members verbatim:
 * - `interface A { m }`            → `type A = { m }`
 * - `interface A extends B { m }`  → `type A = B & { m }`
 * - `interface A extends B {}`     → `type A = B`
 * - sole undocumented call signature → a bare arrow type
 *
 * Pinned to typescript 5.9 (its own package.json): the repo's TS 7 compiler
 * ships no AST API. Usage: `bun convert.ts <file-or-dir>... [--dry]`.
 */
import ts from "typescript"
import * as fs from "node:fs"
import * as path from "node:path"

const args = process.argv.slice(2).filter((a) => !a.startsWith("--"))
const dryRun = process.argv.includes("--dry")

const files: string[] = []
for (const arg of args) {
  const stat = fs.statSync(arg)
  if (stat.isDirectory()) {
    const walk = (dir: string): void => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name)
        if (entry.isDirectory()) walk(full)
        else if (entry.name.endsWith(".ts") && !entry.name.endsWith(".d.ts")) files.push(full)
      }
    }
    walk(arg)
  } else {
    files.push(arg)
  }
}

type Report = {
  file: string
  name: string
  kind: "record" | "arrow" | "intersection-only"
  members: number
  heritage: string[]
}

const reports: Report[] = []
const skipped: string[] = []

const dedent = (text: string, by: number): string => {
  const lines = text.split("\n")
  return lines
    .map((l, i) => (i === 0 ? l : l.replace(new RegExp(`^ {1,${by}}`), "")))
    .join("\n")
}

for (const file of files) {
  const src = fs.readFileSync(file, "utf8")
  const sf = ts.createSourceFile(file, src, ts.ScriptTarget.ESNext, true, ts.ScriptKind.TS)

  const decls: ts.InterfaceDeclaration[] = []
  const walk = (node: ts.Node): void => {
    if (ts.isInterfaceDeclaration(node)) decls.push(node)
    ts.forEachChild(node, walk)
  }
  walk(sf)
  if (decls.length === 0) continue

  type EditT = { start: number; end: number; text: string }
  const edits: EditT[] = []

  for (const node of decls) {
    // Locate the `interface` keyword: after any modifiers (`export`, `declare`).
    const modifiers = ts.canHaveModifiers(node) ? ts.getModifiers(node) : undefined
    const afterModifiers = modifiers && modifiers.length > 0 ? modifiers[modifiers.length - 1]!.end : node.getStart(sf, false)
    const kwStart = src.indexOf("interface", afterModifiers)
    if (kwStart < 0) {
      skipped.push(`${file}: cannot locate keyword for ${node.name.text}`)
      continue
    }

    // Type parameter list verbatim (may be absent).
    const nameEnd = node.name.end
    const heritage = node.heritageClauses?.[0]
    const headerEnd = heritage ? heritage.pos : node.members.pos - 1
    let typeParams = src.slice(nameEnd, headerEnd)
    // Strip the trailing `extends`-less whitespace and any dangling `{`.
    typeParams = typeParams.replace(/\s*\{?\s*$/, "").trim()

    const heritageTexts: string[] = []
    if (node.heritageClauses) {
      for (const clause of node.heritageClauses) {
        for (const t of clause.types) heritageTexts.push(src.slice(t.pos, t.end).trim())
      }
    }

    // Open brace position: the token right before the first member slot.
    const openBrace = src.lastIndexOf("{", node.members.pos)
    if (openBrace < 0) {
      skipped.push(`${file}: cannot locate body for ${node.name.text}`)
      continue
    }

    const header = `type ${node.name.text}${typeParams} = `
    const prefix = heritageTexts.length > 0 ? `${heritageTexts.join(" & ")} & ` : ""

    // Sole call signature with no doc comment of its own → a bare arrow type.
    const sole = node.members.length === 1 ? node.members[0]! : undefined
    const soleCall = sole && ts.isCallSignatureDeclaration(sole) ? sole : undefined
    const soleHasDoc = sole ? /\/\*|\/\//.test(src.slice(sole.pos, sole.getStart(sf, false))) : false

    if (soleCall && !soleHasDoc && heritageTexts.length === 0) {
      const sigTypeParams = soleCall.typeParameters
        ? src
            .slice(
              src.lastIndexOf("<", soleCall.typeParameters.pos),
              src.indexOf(">", soleCall.typeParameters.end) + 1,
            )
            .trim()
        : ""
      const openParen = src.indexOf("(", soleCall.typeParameters ? soleCall.typeParameters.end : soleCall.getStart(sf, false))
      const closeParen = soleCall.type ? src.lastIndexOf(")", soleCall.type.pos) : src.indexOf(")", openParen)
      const paramsText = src.slice(openParen, closeParen + 1)
      const returnText = soleCall.type ? src.slice(soleCall.type.pos, soleCall.type.end).trim() : "void"
      let arrow = `${header}${sigTypeParams}${paramsText} => ${returnText}`
      if (arrow.includes("\n")) arrow = dedent(arrow, 2)
      edits.push({ start: kwStart, end: node.end, text: arrow })
      reports.push({ file, name: node.name.text, kind: "arrow", members: 1, heritage: heritageTexts })
      continue
    }

    if (node.members.length === 0 && heritageTexts.length > 0) {
      edits.push({ start: kwStart, end: node.end, text: `${header}${heritageTexts.join(" & ")}` })
      reports.push({ file, name: node.name.text, kind: "intersection-only", members: 0, heritage: heritageTexts })
      continue
    }

    edits.push({ start: kwStart, end: openBrace + 1, text: `${header}${prefix}{` })
    reports.push({
      file,
      name: node.name.text,
      kind: "record",
      members: node.members.length,
      heritage: heritageTexts,
    })
  }

  edits.sort((a, b) => b.start - a.start)
  let out = src
  for (const e of edits) out = out.slice(0, e.start) + e.text + out.slice(e.end)
  if (!dryRun && out !== src) fs.writeFileSync(file, out)
}

console.log(JSON.stringify({ files: files.length, converted: reports.length, skipped }, null, 1))
