from plotly import graph_objects as go

from labox.extra.plotly import FigureSerializer
from tests.core_serializer_utils import make_value_serializer_test

test_plotly_value_serializer = make_value_serializer_test(
    FigureSerializer(),
    go.Figure(go.Scatter(x=[1, 2, 3], y=[4, 5, 6])),
)
