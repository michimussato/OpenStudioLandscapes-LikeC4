import textwrap

import snakemd


def readme_feature(
    doc: snakemd.Document,
    main_header: str,
) -> snakemd.Document:

    # Some Specific information

    doc.add_heading(
        text=main_header,
        level=1,
    )

    # Logo

    doc.add_paragraph(
        snakemd.Inline(
            text=textwrap.dedent(
                """\
                Logo LikeC4\
                """
            ),
            image="https://likec4.dev/_astro/logo-dark.h9QdQ6Li.svg",
            link="https://likec4.dev/",
        ).__str__()
    )

    doc.add_paragraph(
        text=textwrap.dedent(
            """\
            Design, visualize, and maintain documentation with a modern 
            [DSL](https://en.wikipedia.org/wiki/Domain-specific_language). 
            Version controlled with your code: 
            Architecture-as-Code.\
            """
        )
    )

    doc.add_paragraph(
        text=textwrap.dedent(
            """\
            LikeC4 is an open source architecture modeling tool based
            on the [C4 Model](https://structurizr.com/).\
            """
        )
    )

    doc.add_heading(
        text="Official Documentation",
        level=2,
    )

    doc.add_unordered_list(
        [
            "[Website](https://likec4.dev/)",
            "[Docs](https://likec4.dev/tutorial/)",
            "[Playground](https://playground.likec4.dev/w/tutorial/index/)",
            "[GitHub](https://github.com/likec4/likec4)"
        ]
    )

    doc.add_horizontal_rule()

    return doc


if __name__ == "__main__":
    pass
