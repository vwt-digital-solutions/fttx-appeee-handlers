class FormEntryService:
    @staticmethod
    def convert_form_entry_to_gis_object(form_entry_collection):
        """
        Convert form entry to formatted GIS object

        :param form_entry_collection: Form entry collection
        :type form_entry_collection: dict

        :return GIS object
        :rtype: dict
        """

        form_entry = form_entry_collection["features"][0]
        form_entry_properties = form_entry["properties"]

        return {
            "geometry": {
                "x": form_entry["geometry"]["coordinates"][1],
                "y": form_entry["geometry"]["coordinates"][0],
            },
            "attributes": {
                "sleutel": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["FCA_SLEUTEL"],
                "postcode": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["DSS"][:6],
                "huisnummer": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["DSS"][6:],
                "bon_naam": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["BONNAAM"],
                "fca_project": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["FCA_PROJECT"],
                "lat_wgs84": form_entry["geometry"]["coordinates"][0],
                "lon_wgs84": form_entry["geometry"]["coordinates"][1],
            },
        }, form_entry_properties["Entry"]["AnswersJson"]["AFRONDEN_PAGE"][
            "FCA_FOTO_GEVEL"
        ]
