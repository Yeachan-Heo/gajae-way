export type SqlClassification = { readonly kind: "read" } | { readonly kind: "write"; readonly table: string };

interface Token {
	readonly value: string;
	readonly kind: "word" | "identifier" | "string" | "punctuation";
}

function isIdentifierStart(char: string): boolean {
	return /[A-Za-z_$]/.test(char) || char.charCodeAt(0) > 127;
}

function isIdentifierPart(char: string): boolean {
	return /[A-Za-z0-9_$]/.test(char) || char.charCodeAt(0) > 127;
}

function tokenize(sql: string): Token[] | undefined {
	const tokens: Token[] = [];
	for (let index = 0; index < sql.length; ) {
		const char = sql[index]!;
		if (/\s/.test(char)) {
			index++;
			continue;
		}
		if (char === "-" && sql[index + 1] === "-") {
			index += 2;
			while (index < sql.length && sql[index] !== "\n" && sql[index] !== "\r") index++;
			continue;
		}
		if (char === "/" && sql[index + 1] === "*") {
			const end = sql.indexOf("*/", index + 2);
			if (end === -1) return undefined;
			index = end + 2;
			continue;
		}
		if (char === "'" || char === '"' || char === "`") {
			const quote = char;
			let segmentStart = ++index;
			let value = "";
			let closed = false;
			while (index < sql.length) {
				if (sql[index] === quote) {
					if (sql[index + 1] === quote) {
						value += sql.slice(segmentStart, index) + quote;
						index += 2;
						segmentStart = index;
						continue;
					}
					value += sql.slice(segmentStart, index);
					index++;
					closed = true;
					break;
				}
				index++;
			}
			if (!closed) return undefined;
			tokens.push({ value, kind: quote === "'" ? "string" : "identifier" });
			continue;
		}
		if (char === "[") {
			const end = sql.indexOf("]", index + 1);
			if (end === -1) return undefined;
			tokens.push({ value: sql.slice(index + 1, end), kind: "identifier" });
			index = end + 1;
			continue;
		}
		if (isIdentifierStart(char)) {
			const start = index++;
			while (index < sql.length && isIdentifierPart(sql[index]!)) index++;
			tokens.push({ value: sql.slice(start, index), kind: "word" });
			continue;
		}
		tokens.push({ value: char, kind: "punctuation" });
		index++;
	}
	return tokens;
}

function keyword(token: Token | undefined, expected: string): boolean {
	return token?.kind === "word" && token.value.toUpperCase() === expected;
}

function isName(token: Token | undefined): token is Token {
	return token !== undefined && (token.kind === "word" || token.kind === "identifier");
}

function consumeBalanced(tokens: readonly Token[], start: number): number | undefined {
	if (tokens[start]?.value !== "(") return undefined;
	let depth = 0;
	for (let index = start; index < tokens.length; index++) {
		if (tokens[index]?.value === "(") depth++;
		else if (tokens[index]?.value === ")") {
			depth--;
			if (depth === 0) return index + 1;
			if (depth < 0) return undefined;
		}
	}
	return undefined;
}

function skipWithPrefix(tokens: readonly Token[]): number | undefined {
	if (!keyword(tokens[0], "WITH")) return 0;
	let index = 1;
	if (keyword(tokens[index], "RECURSIVE")) index++;
	for (;;) {
		if (!isName(tokens[index])) return undefined;
		index++;
		if (tokens[index]?.value === "(") {
			const afterColumns = consumeBalanced(tokens, index);
			if (afterColumns === undefined) return undefined;
			index = afterColumns;
		}
		if (!keyword(tokens[index], "AS")) return undefined;
		index++;
		if (keyword(tokens[index], "NOT") && keyword(tokens[index + 1], "MATERIALIZED")) index += 2;
		else if (keyword(tokens[index], "MATERIALIZED")) index++;
		const afterBody = consumeBalanced(tokens, index);
		if (afterBody === undefined) return undefined;
		index = afterBody;
		if (tokens[index]?.value !== ",") return index;
		index++;
	}
}

function targetTable(tokens: readonly Token[], start: number): { table: string; next: number } | undefined {
	const first = tokens[start];
	if (!isName(first)) return undefined;
	if (tokens[start + 1]?.value !== ".") return { table: first.value.toLowerCase(), next: start + 1 };
	if (first.value.toLowerCase() !== "main") return undefined;
	const table = tokens[start + 2];
	if (!isName(table) || tokens[start + 3]?.value === ".") return undefined;
	return { table: table.value.toLowerCase(), next: start + 3 };
}

/** Classifies only the D3 single-statement SQL subset; undefined means fail closed. */
export function classifySql(sql: string): SqlClassification | undefined {
	const tokens = tokenize(sql);
	if (!tokens || tokens.length === 0) return undefined;
	const semicolons = tokens.flatMap((token, index) => (token.value === ";" ? [index] : []));
	if (semicolons.length > 1 || (semicolons.length === 1 && semicolons[0] !== tokens.length - 1)) return undefined;
	if (semicolons.length === 1) tokens.pop();
	if (tokens.length === 0) return undefined;
	let parentheses = 0;
	for (const token of tokens) {
		if (token.value === "(") parentheses++;
		else if (token.value === ")" && --parentheses < 0) return undefined;
	}
	if (parentheses !== 0) return undefined;

	const commandIndex = skipWithPrefix(tokens);
	if (commandIndex === undefined) return undefined;
	const command = tokens[commandIndex];
	if (keyword(command, "SELECT") || keyword(command, "VALUES")) return { kind: "read" };

	let targetIndex: number | undefined;
	if (keyword(command, "INSERT")) {
		targetIndex = commandIndex + 1;
		if (keyword(tokens[targetIndex], "OR")) targetIndex += 2;
		if (!keyword(tokens[targetIndex], "INTO")) return undefined;
		targetIndex++;
	} else if (keyword(command, "REPLACE")) {
		targetIndex = commandIndex + 1;
		if (!keyword(tokens[targetIndex], "INTO")) return undefined;
		targetIndex++;
	} else if (keyword(command, "UPDATE")) {
		targetIndex = commandIndex + 1;
		if (keyword(tokens[targetIndex], "OR")) targetIndex += 2;
	} else if (keyword(command, "DELETE")) {
		targetIndex = commandIndex + 1;
		if (!keyword(tokens[targetIndex], "FROM")) return undefined;
		targetIndex++;
	} else {
		return undefined;
	}

	const target = targetTable(tokens, targetIndex);
	return target ? { kind: "write", table: target.table } : undefined;
}
