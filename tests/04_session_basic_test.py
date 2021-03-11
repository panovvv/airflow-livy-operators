from airflow import AirflowException
from pytest import raises

from airflow_home.plugins.airflow_livy.session import LivySessionOperator


def test_allowed_statement_kinds():
    kind = "unknown"
    with raises(AirflowException) as ae:
        LivySessionOperator.Statement(kind=kind, code="a=5;")
    print(
        f"\n\nTried to create a statement with kind '{kind}', "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_statement_repr():
    st = LivySessionOperator.Statement(kind="spark", code="a=5;\nprint('s');")
    assert (
        st.__str__()
        == """
{
  Statement, kind: spark
  code:
--------------------------------------------------------------------------------
a=5;
print('s');
--------------------------------------------------------------------------------
}"""
    )


def test_allowed_session_kinds(dag):
    kind = "unknown"
    with raises(AirflowException) as ae:
        LivySessionOperator(
            kind=kind, statements=[], task_id="test_allowed_session_kinds", dag=dag,
        )
    print(
        f"\n\nTried to create a session with kind '{kind}', "
        f"got the expected exception:\n<{ae.value}>"
    )


def test_jinja(dag):
    st1 = LivySessionOperator.Statement(kind="spark", code="x=1+{{ custom_param }};")
    st2 = LivySessionOperator.Statement(
        kind="pyspark", code="print('{{run_id | replace(':', '-')}}')"
    )
    op = LivySessionOperator(
        name="test_jinja_{{ run_id }}",
        statements=[st1, st2],
        task_id="test_jinja_session",
        dag=dag,
    )
    op.render_template_fields({"run_id": "hello:world", "custom_param": 3})
    assert op.name == "test_jinja_hello:world"
    assert op.statements[0].code == "x=1+3;"
    assert op.statements[1].code == "print('hello-world')"
