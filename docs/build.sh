if [ ! -d themes/kube ]; then
    git clone https://github.com/jeblister/kube themes/kube || true
    git checkout 5f68bf3e990eff4108fa251f3a3112d081fffba4
fi

hugo --gc --minify
