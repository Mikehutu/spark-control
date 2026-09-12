"""Recipe registry for Spark Control (slice 3).

A *recipe* is a named model-serving deployment on the cluster: which node(s)
it occupies, the port it serves, how to detect it, and the existing start/stop
scripts the gateway must wrap (never reimplement).

Registry data comes from configuration (dict / env-driven) — recipe facts are
NOT hardcoded in this module. Concrete values live in the deployment's
recipes config file (names, script paths, ports); node addresses resolve via
the node map (env vars).
"""

from __future__ import annotations

from dataclasses import dataclass, field


class RecipeError(Exception):
    """Raised for unknown or misconfigured recipes."""


@dataclass(frozen=True)
class Recipe:
    """One model-serving recipe on the cluster."""

    recipe_id: str
    #: Keys into the node map (e.g. "node-a"); TP2 recipes list both nodes.
    nodes: tuple[str, ...]
    #: Canonical serving port on the head node.
    port: int
    #: Docker compose project (stop is keyed by project — DS text/vision share one).
    compose_project: str
    #: Container name(s) created by the compose project (for detection).
    containers: tuple[str, ...]
    #: Absolute path of the existing start script ON the head node.
    start_script: str
    #: Absolute path of the existing stop script ON the head node.
    stop_script: str
    #: Served model name reported by /v1/models when healthy.
    served_model: str
    #: Head node key (first node by default).
    head_node: str = ""
    #: Start timeout override (model load can take 10+ minutes).
    start_timeout_s: float = 900.0
    #: Recipes that cannot run at the same time (same port/nodes).
    conflicts: tuple[str, ...] = field(default=())

    def head(self) -> str:
        """Node key of the head (serves the API port)."""
        return self.head_node or self.nodes[0]


class RecipeRegistry:
    """Lookup + conflict rules for recipes."""

    def __init__(self, recipes: dict[str, Recipe]) -> None:
        self._recipes = dict(recipes)

    def get(self, recipe_id: str) -> Recipe:
        try:
            return self._recipes[recipe_id]
        except KeyError:
            raise RecipeError(f"unknown recipe: {recipe_id}") from None

    def all(self) -> list[Recipe]:
        return list(self._recipes.values())

    def conflicting(self, recipe: Recipe) -> list[Recipe]:
        """Other recipes that fight *recipe* for the same port or nodes."""
        result = []
        for other in self._recipes.values():
            if other.recipe_id == recipe.recipe_id:
                continue
            same_port = other.port == recipe.port and set(other.nodes) & set(recipe.nodes)
            declared = recipe.recipe_id in other.conflicts or other.recipe_id in recipe.conflicts
            if same_port or declared:
                result.append(other)
        return result

    @classmethod
    def from_config(cls, raw: list[dict[str, object]]) -> RecipeRegistry:
        """Build from plain config dicts (e.g. parsed TOML/JSON/env)."""
        recipes: dict[str, Recipe] = {}
        for entry in raw:
            try:
                nodes_raw = entry["nodes"]
                containers_raw = entry.get("containers", ())
                conflicts_raw = entry.get("conflicts", ())
                if not isinstance(nodes_raw, (list, tuple)):
                    raise RecipeError(f"recipe nodes must be a list: {nodes_raw!r}")
                recipe = Recipe(
                    recipe_id=str(entry["recipe_id"]),
                    nodes=tuple(str(n) for n in nodes_raw),
                    port=int(str(entry["port"])),
                    compose_project=str(entry["compose_project"]),
                    containers=(
                        tuple(str(c) for c in containers_raw)
                        if isinstance(containers_raw, (list, tuple))
                        else ()
                    ),
                    start_script=str(entry["start_script"]),
                    stop_script=str(entry["stop_script"]),
                    served_model=str(entry["served_model"]),
                    head_node=str(entry.get("head_node", "")),
                    start_timeout_s=float(str(entry.get("start_timeout_s", 900.0))),
                    conflicts=(
                        tuple(str(c) for c in conflicts_raw)
                        if isinstance(conflicts_raw, (list, tuple))
                        else ()
                    ),
                )
            except KeyError as exc:
                raise RecipeError(f"recipe config missing key: {exc}") from None
            if not recipe.nodes:
                raise RecipeError(f"recipe {recipe.recipe_id}: nodes must not be empty")
            recipes[recipe.recipe_id] = recipe
        return cls(recipes)
