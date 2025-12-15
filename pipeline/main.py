from pipeline.extract import extract_all
from pipeline.load import load_olap
from pipeline.transform import transform


def main():
    # First extract all
    raw_data = extract_all()

    # then transform
    transformed_data = transform(raw_data)

    # then load
    load_olap(transformed_data)


if __name__ == "__main__":
    main()
