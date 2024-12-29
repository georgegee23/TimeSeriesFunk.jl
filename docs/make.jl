using TimeSeriesFunk
using Documenter

DocMeta.setdocmeta!(TimeSeriesFunk, :DocTestSetup, :(using TimeSeriesFunk); recursive=true)

makedocs(;
    modules=[TimeSeriesFunk],
    authors="georgeg <georgegi86@gmail.com> and contributors",
    sitename="TimeSeriesFunk.jl",
    format=Documenter.HTML(;
        canonical="https://georgegee23.github.io/TimeSeriesFunk.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/georgegee23/TimeSeriesFunk.jl",
    devbranch="master",
)
