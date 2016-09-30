<div class="cmdhead"></div>

<div class="description"></div>

<div class="synopsis"></div>

<div class="options"></div>

<div class="cmdsubsection">
### Description:

Use the [Hail expression language](intro.html#HailExpressionLanguage) to supply a boolean expression involving the following exposed data structures:

Exposed Name | Description
:-: | ---
`v`  | variant
`va` | variant annotation
`global` | global annotation
`gs` | genotype row [aggregable](intro.html#aggregables)

    
For more information about these exposed objects and how to use them, see the documentation on [representation](intro.html#Representation) and the [Hail expression language](intro.html#HailExpressionLanguage).

```
$ hail read -i file.vds
    filtervariants expr -c 'v.contig == "X"' --keep
    ...
```


**Remember:**
 - All variables and values are case sensitive
 - Missing values will always be **excluded**, regardless of `--keep`/`--remove`.  Expressions in which any value is missing will evaluate to missing.

</div>