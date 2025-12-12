from pipeline.extract import extract_all

# from pipeline.transformed import transform
# from pipeline.load import load_olap


def main():
    # First extract all
    raw_data = extract_all()

    # TODO: then transform
    # transformed_data = transform(raw_data)

    # TODO: then load
    # load_olap(transformed_data)


if __name__ == "__main__":
    main()
