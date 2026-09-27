"""Conventions tests: the structural rules from plan.md's section 7.

Every check is AST-based over the discovered files, so a NEW file under
`src/webcrawler` is covered automatically — the parameter lists are the
discovered files themselves, which is also what keeps these tests inside the
every-test-is-parameterized rule they enforce.
"""

import ast
from pathlib import Path
from typing import Final

import pytest

SRC_ROOT: Final = Path(__file__).resolve().parents[1] / "src" / "webcrawler"
TESTS_ROOT: Final = Path(__file__).resolve().parents[1] / "tests"

SOURCE_FILES: Final = tuple(sorted(SRC_ROOT.rglob("*.py")))
TEST_FILES: Final = tuple(sorted(TESTS_ROOT.rglob("test_*.py")))
PORTS_FILES: Final = tuple(sorted((SRC_ROOT / "ports").glob("*.py")))

# The machinery every port class needs, plus the non-project bases
# plan.md's conventions bullet whitelists by module and class.
MACHINERY_BASES: Final = {"ABC", "object"}
WHITELISTED_BASES: Final = {
    ("webcrawler.domain.crawl_state", "CrawlState"): {"str", "Enum"},
    ("webcrawler.domain.custom_url", "InvalidURLError"): {"ValueError"},
    ("webcrawler.domain.messages", "QueueOverflowError"): {"RuntimeError"},
    ("webcrawler.domain.errors", "NonRetryableError"): {"RuntimeError"},
    ("webcrawler.utils.html_parser", "*"): {"HTMLParser"},
    }

# The synchronous port methods allowed by plan.md, each carrying `Synchronous:`.
EXPECTED_SYNC_PORTS: Final = {
    ("PolitenessPolicy", "before_fetch"),
    ("LinkExtractor", "extract"),
    ("TimeProviderFactory", "now"),
    ("RequestDeduplicator", "seen_and_record"),
    ("RequestMiddleware", "apply"),
    }


def module_name(path: Path) -> str:
    """Return the dotted module name of a source file under src/webcrawler.

    Args:
    path: The file to name.

    Returns:
    str: Its dotted name, e.g. `webcrawler.application.worker`.
    """
    parts = (*path.relative_to(SRC_ROOT).with_suffix("").parts,)
    return ".".join(("webcrawler", *parts))


def parsed(path: Path) -> ast.Module:
    """Parse a file once per use, keeping the tests stateless.

    Args:
    path: The file to parse.

    Returns:
    ast.Module: Its parsed tree.
    """
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def project_classes() -> dict[str, str]:
    """Map every project class name to its defining module.

    Returns:
    dict[str, str]: Class name -> dotted module, across src/webcrawler.
    """
    mapping: dict[str, str] = {}
    for path in SOURCE_FILES:
        for node in ast.walk(parsed(path)):
            if isinstance(node, ast.ClassDef):
                mapping[node.name] = module_name(path)
                return mapping


                def is_dunder(name: str) -> bool:
                    """Report whether a name is a dunder.

                    Args:
                    name: The name to check.

                    Returns:
                    bool: True only for `__name__`-style names.
                    """
                    return name.startswith("__") and name.endswith("__")


                def decorators_marked(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
                    """Flatten a function's decorators into readable dotted names.

                    Args:
                    node: The function whose decorators are wanted.

                    Returns:
                    list[str]: One dotted name per decorator.
                    """
                    names: list[str] = []
                    for decorator in node.decorator_list:
                        if isinstance(decorator, ast.Name):
                            names.append(decorator.id())
                        elif isinstance(decorator, ast.Attribute):
                            names.append(decorator.attr())
                        elif isinstance(decorator, ast.Call):
                            names.append(
                                decorator.func.attr()
                                if isinstance(decorator.func, ast.Attribute)
                                else (
                                decorator.func.id()
                                if isinstance(decorator.func, ast.Name)
                                else "unknown"
                                )
                                )
                            return names


                            def returns_none(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
                                """Report whether the function is annotated as returning None.

                                Args:
                                node: The function to inspect.

                                Returns:
                                bool: True when its return annotation is the constant None.
                                """
                                annotation = node.returns
                                return isinstance(annotation, ast.Constant) and annotation.value is None


                            def body_raises(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
                                """Report whether the function's own statements raise.

                                Args:
                                node: The function to inspect.

                                Returns:
                                bool: True when a Raise sits in its body, nested definitions excluded.
                                """
                                for statement in node.body:
                                    for child in ast.walk(statement):
                                        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                                            continue
                                            if isinstance(child, ast.Raise):
                                                return True
                                                return False


                                                def public_definitions(
                                                    tree: ast.Module,
                                                    ) -> list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef]]:
                                                    """Collect this module's public classes, functions, methods, properties.

                                                    Args:
                                                    tree: The parsed module.

                                                    Returns:
                                                    list[tuple[str, ast.FunctionDef | ast.AsyncFunctionDef |
                                                    ast.ClassDef]]: (parent, node) for every public definition, with the
                                                    module's name as the parent of top-level definitions.
                                                    """
                                                    found: list[
                                                        tuple[str, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef]
                                                        ] = []

                                                def visit(
                                                    parent: str,
                                                    body: list[ast.stmt],
                                                    ) -> None:
                                                    for statement in body:
                                                        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
                                                            if statement.name.startswith("_"):
                                                                continue
                                                                found.append((parent, statement))
                                                            elif isinstance(statement, ast.ClassDef):
                                                                if statement.name.startswith("_"):
                                                                    continue
                                                                    found.append((parent, statement))
                                                                    visit(statement.name, statement.body)

                                                                    visit("<module>", tree.body)
                                                                    return found


                                                                    @pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
                                                                    def test_every_public_definition_has_a_non_empty_docstring(path: Path) -> None:
                                                                        """Every public class, function, method, and property is documented.

                                                                        Dunders are excepted (they are excluded by `public_definitions`) and enum
                                                                        members are exempt because CPython discards a member docstring rather
                                                                        than attaching it to the member (plan.md's conventions bullet).

                                                                        Args:
                                                                        path: The source file to inspect.
                                                                        """
                                                                        for _, node in public_definitions(parsed(path)):
                                                                            assert ast.get_docstring(node), f"{path.name}: {getattr(node, 'name', '?')}"


                                                                            @pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
                                                                            def test_docstring_labels_match_the_signature(path: Path) -> None:
                                                                                """`Args:`/`Returns:`/`Raises:` appear exactly when the signature has them.

                                                                                The check is one-directional on `Raises:` — an abstract port may declare
                                                                                a `Raises:` contract its own empty body does not raise (plan.md).

                                                                                Args:
                                                                                path: The source file to inspect.
                                                                                """
                                                                                for _, node in public_definitions(parsed(path)):
                                                                                    if isinstance(node, ast.ClassDef):
                                                                                        continue
                                                                                        docstring = ast.get_docstring(node) or ""
                                                                                        parameters = [
                                                                                            argument
                                                                                            for argument in node.args.args
                                                                                            if argument.arg not in {"self", "cls"}
                                                                                            ]
                                                                                        if parameters:
                                                                                            assert "Args:" in docstring, f"{path.name}: {node.name}: missing Args:"
                                                                                            if not returns_none(node):
                                                                                                assert "Returns:" in docstring, f"{path.name}: {node.name}: missing Returns:"
                                                                                                if body_raises(node):
                                                                                                    assert "Raises:" in docstring, f"{path.name}: {node.name}: missing Raises:"


                                                                                                    @pytest.mark.parametrize(
                                                                                                        "relative",
                                                                                                        ["infrastructure/queue/in_memory_single_topic_single_partition_queue.py"],
                                                                                                        ids=str,
                                                                                                        )
                                                                                                    def test_the_no_lock_rationale_is_in_the_registry_module_docstring(
                                                                                                        relative: str,
                                                                                                        ) -> None:
                                                                                                        """The queue's no-lock rationale lives in the queue module (plan.md).

                                                                                                        Args:
                                                                                                        relative: The file that must carry the rationale.
                                                                                                        """
                                                                                                        docstring = ast.get_docstring(parsed(SRC_ROOT / relative))
                                                                                                        assert docstring and "no lock" in docstring.lower()


                                                                                                    @pytest.mark.parametrize("path", PORTS_FILES, ids=lambda path: path.name)
                                                                                                    def test_every_port_method_is_async_except_the_four_synchronous_ones(
                                                                                                        path: Path,
                                                                                                        ) -> None:
                                                                                                        """Only the four whitelisted sync port methods exist, each marked.

                                                                                                        A new synchronous port method fails here until it is justified with the
                                                                                                        `Synchronous:` marker and added to EXPECTED_SYNC_PORTS (plan.md).

                                                                                                        Args:
                                                                                                        path: The ports module to inspect.
                                                                                                        """
                                                                                                        synchronous: set[tuple[str, str]] = set()
                                                                                                        for parent, node in public_definitions(parsed(path)):
                                                                                                            if not isinstance(parent, str) or parent == "<module>":
                                                                                                                continue
                                                                                                                if not isinstance(node, ast.FunctionDef):
                                                                                                                    continue
                                                                                                                    assert "Synchronous:" in (ast.get_docstring(node) or ""), (
                                                                                                                        f"{path.name}: {parent}.{node.name}: sync port method is unmarked"
                                                                                                                        )
                                                                                                                    synchronous.add((parent, node.name))
                                                                                                                    assert synchronous <= EXPECTED_SYNC_PORTS


                                                                                                                    @pytest.mark.parametrize("path", PORTS_FILES, ids=lambda path: path.name)
                                                                                                                    async def test_the_synchronous_port_exception_set_is_exactly_four(path: Path) -> None:
                                                                                                                        """Across every ports module the sync set is exactly the allowed four.

                                                                                                                        Args:
                                                                                                                        path: The ports module to add to the accumulated set.
                                                                                                                        """
                                                                                                                        synchronous: set[tuple[str, str]] = set()
                                                                                                                        for ports_path in PORTS_FILES:
                                                                                                                            for parent, node in public_definitions(parsed(ports_path)):
                                                                                                                                if isinstance(node, ast.FunctionDef) and parent != "<module>":
                                                                                                                                    synchronous.add((parent, node.name))
                                                                                                                                    assert synchronous == EXPECTED_SYNC_PORTS


                                                                                                                                    @pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
                                                                                                                                    def test_every_class_has_at_most_one_ports_base(path: Path) -> None:
                                                                                                                                        """No domain/application/infrastructure/utils class is ever a base.

                                                                                                                                        The only non-project bases permitted are the machinery (ABC, object)
                                                                                                                                        plus the whitelisted stdlib bases of plan.md's conventions bullet.

                                                                                                                                        Args:
                                                                                                                                        path: The source file to inspect.
                                                                                                                                        """
                                                                                                                                        project = project_classes()
                                                                                                                                        for parent, node in public_definitions(parsed(path)):
                                                                                                                                            if not isinstance(node, ast.ClassDef):
                                                                                                                                                continue
                                                                                                                                                module = module_name(path)
                                                                                                                                                allowed = set(MACHINERY_BASES) | WHITELISTED_BASES.get((module, node.name), set()) | WHITELISTED_BASES.get((module, "*"), set())
                                                                                                                                                project_bases: list[str] = []
                                                                                                                                                for base in node.bases:
                                                                                                                                                    name = base.id if isinstance(base, ast.Name) else (
                                                                                                                                                        base.attr if isinstance(base, ast.Attribute) else ""
                                                                                                                                                        )
                                                                                                                                                    if name in project:
                                                                                                                                                        defining_module = project[name]
                                                                                                                                                        assert defining_module.startswith("webcrawler.ports."), (
                                                                                                                                                            f"{path.name}: {node.name}: project base {name} is not a port"
                                                                                                                                                            )
                                                                                                                                                        project_bases.append(name)
                                                                                                                                                    else:
                                                                                                                                                        assert name in allowed, (
                                                                                                                                                            f"{path.name}: {node.name}: non-project base {name} is not allowed"
                                                                                                                                                            )
                                                                                                                                                        assert len(project_bases) <= 1, (
                                                                                                                                                            f"{path.name}: {node.name}: multiple project bases {project_bases}"
                                                                                                                                                            )


                                                                                                                                                        @pytest.mark.parametrize("path", SOURCE_FILES, ids=lambda path: path.name)
                                                                                                                                                        def test_ports_and_application_do_not_import_inwards(path: Path) -> None:
                                                                                                                                                            """ports/ and application/ import no infrastructure/utils, bar the root.

                                                                                                                                                            Only `main.py`, the composition root, may import concrete
                                                                                                                                                            implementations (plan.md).

                                                                                                                                                            Args:
                                                                                                                                                            path: The source file to inspect.
                                                                                                                                                            """
                                                                                                                                                            module = module_name(path)
                                                                                                                                                            if not module.startswith(("webcrawler.ports.", "webcrawler.application.")):
                                                                                                                                                                return
                                                                                                                                                                if module.rsplit(".", 1)[-1] == "main":  # the composition root
                                                                                                                                                                    return
                                                                                                                                                                    for node in ast.walk(parsed(path)):
                                                                                                                                                                        if isinstance(node, ast.ImportFrom) and node.module:
                                                                                                                                                                            assert not node.module.startswith(
                                                                                                                                                                                ("webcrawler.infrastructure", "webcrawler.utils")
                                                                                                                                                                                ), f"{path.name}: inward import {node.module}"
                                                                                                                                                                        elif isinstance(node, ast.Import):
                                                                                                                                                                            for alias in node.names:
                                                                                                                                                                                assert not alias.name.startswith(
                                                                                                                                                                                    ("webcrawler.infrastructure", "webcrawler.utils")
                                                                                                                                                                                    ), f"{path.name}: inward import {alias.name}"


                                                                                                                                                                                @pytest.mark.parametrize(
                                                                                                                                                                                    "path",
                                                                                                                                                                                    (
                                                                                                                                                                                    *PORTS_FILES,
                                                                                                                                                                                    SRC_ROOT / "application" / "worker.py",
                                                                                                                                                                                    SRC_ROOT / "application" / "url_poller.py",
                                                                                                                                                                                    ),
                                                                                                                                                                                    ids=lambda path: path.name,
                                                                                                                                                                                    )
                                                                                                                                                                                def test_no_annotation_names_an_infrastructure_symbol(path: Path) -> None:
                                                                                                                                                                                    """Type annotations in ports/worker/poller name no infrastructure symbol.

                                                                                                                                                                                    Args:
                                                                                                                                                                                    path: The source file to inspect.
                                                                                                                                                                                    """
                                                                                                                                                                                    infrastructure_names: set[str] = set()
                                                                                                                                                                                    for source in (SRC_ROOT / "infrastructure").rglob("*.py"):
                                                                                                                                                                                        for node in parsed(source).body:
                                                                                                                                                                                            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                                                                                                                                                                                                infrastructure_names.add(node.name())
                                                                                                                                                                                                for node in ast.walk(parsed(path)):
                                                                                                                                                                                                    if isinstance(node, ast.Name) and node.id in infrastructure_names:
                                                                                                                                                                                                        raise AssertionError(f"{path.name}: annotation names {node.id}")
                                                                                                                                                                                                        if (
                                                                                                                                                                                                            isinstance(node, ast.Attribute)
                                                                                                                                                                                                            and node.attr in infrastructure_names
                                                                                                                                                                                                            and isinstance(node.value, ast.Name)
                                                                                                                                                                                                            and node.value.id == "infrastructure"
                                                                                                                                                                                                            ):
                                                                                                                                                                                                            raise AssertionError(f"{path.name}: annotation names {node.attr}")


                                                                                                                                                                                                            @pytest.mark.parametrize(
                                                                                                                                                                                                                "relative", ["infrastructure/db/sqlite_url_state_repository.py"], ids=str
                                                                                                                                                                                                                )
                                                                                                                                                                                                            def test_a_claim_issues_begin_immediate_before_its_update(relative: str) -> None:
                                                                                                                                                                                                                """The claim transaction opens BEGIN IMMEDIATE before the update runs.

                                                                                                                                                                                                                Structural check only: the repository test file proves the behaviour.

                                                                                                                                                                                                                Args:
                                                                                                                                                                                                                relative: The file whose transaction helper must be ordered.
                                                                                                                                                                                                                """
                                                                                                                                                                                                                source = (SRC_ROOT / relative).read_text(encoding="utf-8")
                                                                                                                                                                                                                transaction = source.split("async def _transaction", 1)[1]
                                                                                                                                                                                                                begin = transaction.index("BEGIN_IMMEDIATE_SQL")
                                                                                                                                                                                                                update = transaction.index("await body()")
                                                                                                                                                                                                                assert begin < update


                                                                                                                                                                                                            @pytest.mark.parametrize("relative", ["application/orchestrator.py"], ids=str)
                                                                                                                                                                                                            def test_the_orchestrator_closes_the_repository_in_a_finally(relative: str) -> None:
                                                                                                                                                                                                                """repository.close() is awaited from a finally block (plan.md).

                                                                                                                                                                                                                Args:
                                                                                                                                                                                                                relative: The file whose run() must close on every exit.
                                                                                                                                                                                                                """
                                                                                                                                                                                                                source = (SRC_ROOT / relative).read_text(encoding="utf-8")
                                                                                                                                                                                                                run = source.split("async def run", 1)[1]
                                                                                                                                                                                                                close = run.index("await self._repository.close()")
                                                                                                                                                                                                                finally_keyword = run.index("finally:")
                                                                                                                                                                                                                assert finally_keyword < close


                                                                                                                                                                                                            @pytest.mark.parametrize("path", TEST_FILES, ids=lambda path: path.name)
                                                                                                                                                                                                            def test_every_test_function_is_parameterized(path: Path) -> None:
                                                                                                                                                                                                                """Every test function in tests/ carries pytest.mark.parametrize.

                                                                                                                                                                                                                Args:
                                                                                                                                                                                                                path: The test file to inspect.
                                                                                                                                                                                                                """
                                                                                                                                                                                                                for node in ast.walk(parsed(path)):
                                                                                                                                                                                                                    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                                                                                                                                                                                                                        continue
                                                                                                                                                                                                                        if not node.name.startswith("test_"):
                                                                                                                                                                                                                            continue
                                                                                                                                                                                                                            assert "parametrize" in decorators_marked(node), (
                                                                                                                                                                                                                                f"{path.name}: {node.name}: missing pytest.mark.parametrize"
                                                                                                                                                                                                                                )


                                                                                                                                                                                                                            @pytest.mark.parametrize("relative", ["README.md"], ids=str)
                                                                                                                                                                                                                            def test_the_cdc_rationale_is_in_the_readme(relative: str) -> None:
                                                                                                                                                                                                                                """The README states the queue-timeout recovery in CDC terms (plan.md).

                                                                                                                                                                                                                                Args:
                                                                                                                                                                                                                                relative: The file that must carry the CDC rationale.
                                                                                                                                                                                                                                """
                                                                                                                                                                                                                                text = (TESTS_ROOT.parent / relative).read_text(encoding="utf-8")
                                                                                                                                                                                                                                assert "queue_timeout" in text
                                                                                                                                                                                                                                assert "queued" in text
