# TODO

Migrating Pub/Subs

    find . -type f -iname '*.go' -exec sed -i -E "s/github\.com\/ThreeDotsLabs\/watermill\/message\/infrastructure\/([a-z]+)/github.com\/ThreeDotsLabs\/watermill-\1\/pkg\/\1/" "{}" +;